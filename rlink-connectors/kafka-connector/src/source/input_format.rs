use std::borrow::BorrowMut;
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{ClientConfig, Offset};
use rlink::api::backend::OperatorStateBackend;
use rlink::api::checkpoint::CheckpointedFunction;
use rlink::api::element::Record;
use rlink::api::function::{
    Context, InputFormat, InputSplit, InputSplitAssigner, InputSplitSource,
};
use rlink::api::properties::{Properties, SystemProperties};
use rlink::channel::TryRecvError;

use crate::source::checkpoint::KafkaCheckpointed;
use crate::source::consumer::{create_kafka_consumer, get_kafka_consumer_handover};
use crate::source::handover::Handover;
use crate::KafkaRecord;

#[derive(Function)]
pub struct KafkaInputFormat {
    client_config: ClientConfig,
    topics: Vec<String>,
    handover: Option<Handover>,

    state_mode: Option<OperatorStateBackend>,
    checkpoint: Option<KafkaCheckpointed>,

    counter: u64,
}

impl KafkaInputFormat {
    pub fn new(client_config: ClientConfig, topics: Vec<String>) -> Self {
        KafkaInputFormat {
            client_config,
            topics,
            handover: None,
            state_mode: None,
            checkpoint: None,
            counter: 0,
        }
    }
}

impl InputFormat for KafkaInputFormat {
    fn open(&mut self, input_split: InputSplit, context: &Context) {
        info!("kafka source open");

        let can_create_consumer = input_split
            .get_properties()
            .get_string("create_kafka_connection")
            .unwrap();
        if can_create_consumer.to_lowercase().eq("true") {
            let state_backend = context
                .application_properties
                .get_operator_state_backend()
                .unwrap_or(OperatorStateBackend::None);
            self.state_mode = Some(state_backend);

            let mut kafka_checkpoint = KafkaCheckpointed::new(
                context.application_id.clone(),
                context.task_id.job_id(),
                context.task_id.task_number(),
            );
            // todo provide the data from coordinator
            kafka_checkpoint.initialize_state(
                &context.get_checkpoint_context(),
                &context.checkpoint_handle,
            );
            self.checkpoint = Some(kafka_checkpoint);

            let topic = input_split.get_properties().get_string("topic").unwrap();
            let partition = input_split.get_properties().get_i32("partition").unwrap();

            self.handover = Some(Handover::new(topic.as_str(), partition));

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
    }

    fn reached_end(&self) -> bool {
        false
    }

    fn next_record(&mut self) -> Option<Record> {
        match self.handover.as_ref() {
            Some(handover) => {
                match handover.poll_next() {
                    Ok(mut record) => {
                        // save to state
                        let mut reader = KafkaRecord::new(record.borrow_mut());

                        // same as `self.counter % 4096`
                        if self.counter & 4095 == 0 {
                            match self.checkpoint.as_mut() {
                                Some(checkpoint) => {
                                    checkpoint.get_state().update(
                                        reader.get_kafka_topic().unwrap(),
                                        reader.get_kafka_partition().unwrap(),
                                        reader.get_kafka_offset().unwrap(),
                                    );
                                }
                                None => {}
                            }
                        }

                        self.counter += 1;

                        Some(record)
                    }
                    Err(TryRecvError::Empty) => {
                        self.counter = 0;
                        None
                    }
                    Err(TryRecvError::Disconnected) => {
                        panic!("kafka input recv channel disconnected");
                    }
                }
            }
            None => None,
        }
    }

    fn close(&mut self) {}

    fn get_checkpoint(&mut self) -> Option<Box<&mut dyn CheckpointedFunction>> {
        match self.checkpoint.as_mut() {
            Some(checkpoint) => Some(Box::new(checkpoint)),
            None => None,
        }
    }
}

impl InputSplitSource for KafkaInputFormat {
    fn create_input_splits(&self, min_num_splits: u32) -> Vec<InputSplit> {
        let timeout = Duration::from_secs(3);

        info!("kafka config {:?}", self.client_config);

        let consumer: BaseConsumer = self
            .client_config
            .create()
            .expect("Consumer creation failed");

        let mut input_splits = Vec::new();
        let mut index = 0;
        for topic in &self.topics {
            let metadata = consumer
                .fetch_metadata(Some(topic.as_str()), timeout)
                .expect("Failed to fetch metadata");
            let metadata_topic = metadata
                .topics()
                .get(0)
                .expect(format!("Topic({}) not found", topic).as_str());

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
            panic!("kafka `input_splits.len()` != `min_num_splits`")
        }

        if input_splits.len() < min_num_splits as usize {
            let mut extend_input_splits = Vec::new();
            let times = (min_num_splits as usize + input_splits.len() - 1) / input_splits.len();
            for _ in 1..times {
                for input_split in &input_splits {
                    let split_number = input_split.get_split_number();
                    let mut properties = input_split.get_properties().clone();
                    properties.set_str("create_kafka_connection", "false");

                    extend_input_splits.push(InputSplit::new(split_number, properties));
                }
            }
            input_splits.extend_from_slice(extend_input_splits.as_slice());
        }

        input_splits
    }

    fn get_input_split_assigner(&self, _input_splits: Vec<InputSplit>) -> InputSplitAssigner {
        unimplemented!()
    }
}
