use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Offset};
use rlink::api;
use rlink::api::checkpoint::CheckpointFunction;
use rlink::api::element::Record;
use rlink::api::function::{Context, InputFormat, InputSplit, InputSplitSource};
use rlink::api::properties::Properties;
use rlink::channel::handover::Handover;
use rlink::metrics::Tag;

use crate::source::checkpoint::KafkaCheckpointFunction;
use crate::source::consumer::{create_kafka_consumer, get_kafka_consumer_handover};
use crate::source::iterator::KafkaRecordIterator;

#[derive(Function)]
pub struct KafkaInputFormat {
    client_config: ClientConfig,
    topics: Vec<String>,

    buffer_size: usize,
    handover: Option<Handover>,

    checkpoint: Option<KafkaCheckpointFunction>,
}

impl KafkaInputFormat {
    pub fn new(client_config: ClientConfig, topics: Vec<String>, buffer_size: usize) -> Self {
        KafkaInputFormat {
            client_config,
            topics,
            buffer_size,
            handover: None,
            checkpoint: None,
        }
    }
}

impl InputFormat for KafkaInputFormat {
    fn open(&mut self, input_split: InputSplit, context: &Context) -> api::Result<()> {
        info!("kafka source open");

        let can_create_consumer = input_split
            .properties()
            .get_string("create_kafka_connection")?;
        if can_create_consumer.to_lowercase().eq("true") {
            let mut kafka_checkpoint =
                KafkaCheckpointFunction::new(context.application_id.clone(), context.task_id);
            // todo provide the data from coordinator
            kafka_checkpoint
                .initialize_state(&context.checkpoint_context(), &context.checkpoint_handle);
            self.checkpoint = Some(kafka_checkpoint);

            let topic = input_split.properties().get_string("topic").unwrap();
            let partition = input_split.properties().get_i32("partition").unwrap();

            let tags = vec![
                Tag::from(("topic", topic.as_str())),
                Tag::from(("partition", partition)),
            ];
            self.handover = Some(Handover::new(
                "KafkaSource_Handover",
                tags,
                self.buffer_size,
            ));

            let partition_offset =
                self.checkpoint
                    .as_mut()
                    .unwrap()
                    .get_state()
                    .get(topic, partition, Offset::End);

            let client_config = self.client_config.clone();
            let handover = self.handover.as_ref().unwrap().clone();
            let partition_offsets = vec![partition_offset];
            create_kafka_consumer(
                context.task_id.job_id(),
                context.task_id.task_number(),
                client_config,
                partition_offsets,
                handover,
            );

            info!("start with consumer and operator mode")
        } else {
            self.handover = get_kafka_consumer_handover(context.task_id.job_id());

            info!("start with follower operator mode")
        }

        Ok(())
    }

    fn record_iter(&mut self) -> Box<dyn Iterator<Item = Record> + Send> {
        Box::new(KafkaRecordIterator::new(
            self.handover.as_ref().unwrap().clone(),
            self.checkpoint.as_ref().unwrap().clone(),
        ))
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }

    fn checkpoint_function(&mut self) -> Option<Box<&mut dyn CheckpointFunction>> {
        match self.checkpoint.as_mut() {
            Some(checkpoint) => Some(Box::new(checkpoint)),
            None => None,
        }
    }
}

impl InputSplitSource for KafkaInputFormat {
    fn create_input_splits(&self, min_num_splits: u16) -> api::Result<Vec<InputSplit>> {
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
                properties.set_str("create_kafka_connection", "true");

                let input_split = InputSplit::new(index, properties);
                index += 1;

                input_splits.push(input_split);
                if index == min_num_splits {
                    break;
                }
            }
        }

        if input_splits.len() > min_num_splits as usize {
            return Err(rlink::api::Error::from(
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
                    properties.set_str("create_kafka_connection", "false");

                    extend_input_splits.push(InputSplit::new(split_number, properties));
                }
            }
            input_splits.extend_from_slice(extend_input_splits.as_slice());
        }

        Ok(input_splits)
    }
}
