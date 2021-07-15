use std::collections::HashMap;
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Offset};
use rlink::channel::utils::handover::Handover;
use rlink::core;
use rlink::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use rlink::core::element::Record;
use rlink::core::function::{Context, InputFormat, InputSplit, InputSplitSource};
use rlink::core::properties::Properties;
use rlink::metrics::Tag;

use crate::source::checkpoint::KafkaCheckpointFunction;
use crate::source::consumer::{create_kafka_consumer, ConsumerRange};
use crate::source::deserializer::KafkaRecordDeserializerBuilder;
use crate::source::iterator::KafkaRecordIterator;
use crate::state::PartitionOffset;

const CREATE_KAFKA_CONNECTION: &'static str = "create_kafka_connection";

#[derive(NamedFunction)]
pub struct KafkaInputFormat {
    client_config: ClientConfig,
    topics: Vec<String>,

    buffer_size: usize,
    begin_offset: Option<HashMap<String, Vec<PartitionOffset>>>,
    end_offset: Option<HashMap<String, Vec<PartitionOffset>>>,

    handover: Option<Handover>,

    deserializer_builder: Box<dyn KafkaRecordDeserializerBuilder>,

    checkpoint: Option<KafkaCheckpointFunction>,
}

impl KafkaInputFormat {
    pub fn new(
        client_config: ClientConfig,
        topics: Vec<String>,
        buffer_size: usize,
        begin_offset: Option<HashMap<String, Vec<PartitionOffset>>>,
        end_offset: Option<HashMap<String, Vec<PartitionOffset>>>,
        deserializer_builder: Box<dyn KafkaRecordDeserializerBuilder>,
    ) -> Self {
        KafkaInputFormat {
            client_config,
            topics,
            buffer_size,
            begin_offset,
            end_offset,
            handover: None,
            checkpoint: None,
            deserializer_builder,
        }
    }

    fn consumer_ranges(&mut self, topic: String, partition: i32) -> Vec<ConsumerRange> {
        let begin_partition_offsets = self.begin_partition_offset(topic.as_str(), partition);
        let end_partition_offset = self.end_partition_offset(topic.as_str(), partition);

        let consumer_range = ConsumerRange {
            topic,
            partition,
            begin_offset: begin_partition_offsets.offset,
            end_offset: end_partition_offset.map(|x| x.offset),
        };
        vec![consumer_range]
    }

    fn end_partition_offset(&self, topic: &str, partition: i32) -> Option<PartitionOffset> {
        let end_offset = self.end_offset.as_ref()?;
        let pos = end_offset.get(topic)?;
        pos.get(partition as usize).map(|x| x.clone())
    }

    /// get the offset of specified partition
    /// 1. try get from checkpoint state
    /// 2. try get from user specified offset
    /// `Offset::End` otherwise
    fn begin_partition_offset(&mut self, topic: &str, partition: i32) -> PartitionOffset {
        let partition_offset = {
            let state = self.checkpoint.as_mut().unwrap().get_state();
            state.get(topic, partition)
        };

        partition_offset.unwrap_or_else(|| {
            self.get_begin_partition_offset(topic, partition)
                .unwrap_or(PartitionOffset {
                    partition,
                    offset: Offset::End.to_raw().unwrap(),
                })
        })
    }

    fn get_begin_partition_offset(&self, topic: &str, partition: i32) -> Option<PartitionOffset> {
        let begin_offset = self.begin_offset.as_ref()?;
        let partition_offsets = begin_offset.get(topic)?;
        partition_offsets.get(partition as usize).map(|p| p.clone())
    }
}

impl InputFormat for KafkaInputFormat {
    fn open(&mut self, input_split: InputSplit, context: &Context) -> core::Result<()> {
        info!("kafka source open");

        let kafka_checkpoint =
            KafkaCheckpointFunction::new(context.application_id.clone(), context.task_id);
        self.checkpoint = Some(kafka_checkpoint);

        self.initialize_state(&context.checkpoint_context(), &context.checkpoint_handle);

        let topic = input_split.properties().get_string("topic").unwrap();
        let partition = input_split.properties().get_i32("partition").unwrap();

        let tags = vec![
            Tag::new("topic", topic.as_str()),
            Tag::new("partition", partition),
        ];
        self.handover = Some(Handover::new(
            "KafkaSource_Handover",
            tags,
            self.buffer_size,
        ));

        let client_config = self.client_config.clone();
        let handover = self.handover.as_ref().unwrap().clone();

        let consumer_ranges = self.consumer_ranges(topic, partition);
        create_kafka_consumer(
            context.task_id.job_id(),
            context.task_id.task_number(),
            client_config,
            consumer_ranges,
            handover,
            self.deserializer_builder.build(),
        );

        info!("start with consumer and operator mode");

        Ok(())
    }

    fn record_iter(&mut self) -> Box<dyn Iterator<Item = Record> + Send> {
        let handover = self.handover.as_ref().unwrap().clone();
        let state_recorder = self.checkpoint.as_mut().unwrap().get_state().clone();
        Box::new(KafkaRecordIterator::new(handover, state_recorder))
    }

    fn close(&mut self) -> core::Result<()> {
        Ok(())
    }
}

impl CheckpointFunction for KafkaInputFormat {
    fn initialize_state(
        &mut self,
        context: &FunctionSnapshotContext,
        handle: &Option<CheckpointHandle>,
    ) {
        self.checkpoint
            .as_mut()
            .unwrap()
            .initialize_state(context, handle);
    }

    /// trigger the method when the `operator` operate a `Barrier` event
    fn snapshot_state(&mut self, context: &FunctionSnapshotContext) -> Option<CheckpointHandle> {
        match self.checkpoint.as_mut() {
            Some(checkpoint) => checkpoint.snapshot_state(context),
            None => None,
        }
    }
}

impl InputSplitSource for KafkaInputFormat {
    fn create_input_splits(&self, min_num_splits: u16) -> core::Result<Vec<InputSplit>> {
        let timeout = Duration::from_secs(3);

        info!("kafka config {:?}", self.client_config);

        let consumer: BaseConsumer = self
            .client_config
            .create()
            .map_err(|e| anyhow!("Consumer creation failed. {}", e))?;

        let mut input_splits = Vec::new();
        let mut index = 0;
        for topic in &self.topics {
            let metadata = consumer
                .fetch_metadata(Some(topic.as_str()), timeout)
                .map_err(|e| anyhow!("Failed to fetch metadata. {}", e))?;
            let metadata_topic = metadata
                .topics()
                .get(0)
                .ok_or(anyhow!("Topic({}) not found", topic))?;

            for partition in metadata_topic.partitions() {
                let mut properties = Properties::new();
                properties.set_str("topic", topic.as_str());
                properties.set_i32("partition", partition.id());
                properties.set_bool(CREATE_KAFKA_CONNECTION, true);

                let input_split = InputSplit::new(index, properties);
                index += 1;

                input_splits.push(input_split);
                if index == min_num_splits {
                    break;
                }
            }
        }

        if input_splits.len() > min_num_splits as usize {
            return Err(rlink::core::Error::from(
                "kafka `input_splits.len()` != `min_num_splits`",
            ));
        }

        if input_splits.len() < min_num_splits as usize {
            let mut extend_input_splits = Vec::new();
            let times = (min_num_splits as usize + input_splits.len() - 1) / input_splits.len();
            for _ in 1..times {
                for input_split in &input_splits {
                    let split_number = input_split.split_number();
                    let mut properties = input_split.properties().clone();
                    properties.set_bool(CREATE_KAFKA_CONNECTION, false);

                    extend_input_splits.push(InputSplit::new(split_number, properties));
                }
            }
            input_splits.extend_from_slice(extend_input_splits.as_slice());
        }

        Ok(input_splits)
    }
}
