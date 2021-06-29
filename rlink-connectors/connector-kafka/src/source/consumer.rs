use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use futures::StreamExt;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use rlink::channel::utils::handover::Handover;
use rlink::core::runtime::JobId;
use rlink::utils;
use rlink::utils::thread::async_runtime;

use crate::source::deserializer::KafkaRecordDeserializer;
use crate::state::PartitionOffset;

struct TaskHandover {
    task_number: u16,
    handover: Handover,
    subscriptions: usize,
}

impl TaskHandover {
    pub fn new(task_number: u16, handover: Handover) -> Self {
        TaskHandover {
            task_number,
            handover,
            subscriptions: 1,
        }
    }
}

lazy_static! {
    static ref KAFKA_CONSUMERS: Mutex<HashMap<JobId, Vec<TaskHandover>>> =
        Mutex::new(HashMap::new());
}

pub(crate) fn create_kafka_consumer(
    job_id: JobId,
    task_number: u16,
    client_config: ClientConfig,
    partition_offsets: Vec<(String, PartitionOffset)>,
    handover: Handover,
    deserializer: Box<dyn KafkaRecordDeserializer>,
) {
    let kafka_consumer: &Mutex<HashMap<JobId, Vec<TaskHandover>>> = &*KAFKA_CONSUMERS;
    let mut kafka_consumer = kafka_consumer.lock().unwrap();

    let task_handovers = kafka_consumer.entry(job_id).or_insert(Vec::new());
    if task_handovers
        .iter()
        .find(|x| x.task_number == task_number)
        .is_some()
    {
        panic!("repeat create kafka consumer");
    }

    let handover_clone = handover.clone();
    utils::thread::spawn("kafka-source-block", move || {
        async_runtime("kafka_source").block_on(async {
            let mut kafka_consumer = KafkaConsumerThread::new(
                client_config,
                partition_offsets,
                handover_clone,
                deserializer,
            );
            match kafka_consumer.run().await {
                Ok(()) => {}
                Err(e) => {
                    error!("run consumer error. {}", e);
                }
            }
        });
    });

    task_handovers.push(TaskHandover::new(task_number, handover));
}

pub(crate) fn get_kafka_consumer_handover(job_id: JobId) -> Option<Handover> {
    // todo why??? for debug?
    std::thread::sleep(Duration::from_secs(5));

    let kafka_consumer: &Mutex<HashMap<JobId, Vec<TaskHandover>>> = &*KAFKA_CONSUMERS;
    for _ in 0..5 {
        let mut kafka_consumer = kafka_consumer.lock().unwrap();
        match kafka_consumer.get_mut(&job_id) {
            Some(task_handover) => {
                return match task_handover.iter_mut().min_by_key(|x| x.subscriptions) {
                    Some(task_handover) => {
                        task_handover.subscriptions += 1;
                        info!("subscript from task_number={}", task_handover.task_number);
                        Some(task_handover.handover.clone())
                    }
                    None => None,
                }
            }
            None => std::thread::sleep(Duration::from_secs(1)),
        }
    }

    None
}

pub struct KafkaConsumerThread {
    client_config: ClientConfig,
    partition_offsets: Vec<(String, PartitionOffset)>,

    handover: Handover,
    deserializer: Box<dyn KafkaRecordDeserializer>,
}

impl KafkaConsumerThread {
    pub fn new(
        client_config: ClientConfig,
        partition_offsets: Vec<(String, PartitionOffset)>,
        handover: Handover,
        deserializer: Box<dyn KafkaRecordDeserializer>,
    ) -> Self {
        KafkaConsumerThread {
            client_config,
            partition_offsets,
            handover,
            deserializer,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let mut assignment = TopicPartitionList::new();
        for (topic, partition_offset) in &self.partition_offsets {
            assignment
                .add_partition_offset(
                    topic.as_str(),
                    partition_offset.partition,
                    Offset::from_raw(partition_offset.offset),
                )
                .unwrap();
        }

        // let group_id = format!("rlink{}", Uuid::new_v4());
        self.client_config
            .get("group.id")
            .ok_or(anyhow!("`group.id` not found in kafka consumer config"))?;

        let consumer: StreamConsumer<DefaultConsumerContext> = self.client_config.create()?;
        consumer.assign(&assignment)?;

        info!(
            "create consumer success. config: {:?}, apply checkpoint offset: {:?}",
            self.client_config, self.partition_offsets
        );

        let mut message_stream = consumer.stream();
        while let Some(message) = message_stream.next().await {
            match message {
                Ok(borrowed_message) => {
                    let topic = borrowed_message.topic();
                    let partition = borrowed_message.partition();
                    let offset = borrowed_message.offset();
                    let timestamp = borrowed_message.timestamp().to_millis().unwrap_or(0);
                    let key = borrowed_message.key().unwrap_or(&utils::EMPTY_SLICE);
                    let payload = borrowed_message.payload().unwrap_or(&utils::EMPTY_SLICE);

                    let records = self
                        .deserializer
                        .deserialize(timestamp, key, payload, topic, partition, offset);

                    for record in records {
                        match self.handover.produce(record) {
                            Ok(_) => {}
                            Err(_e) => {
                                panic!("handover produce `Disconnected`");
                            }
                        }
                    }
                }
                Err(e) => warn!("Kafka error: {}", e),
            }
        }

        Ok(())
    }
}
