use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use futures::StreamExt;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use rlink::channel::TrySendError;
use rlink::utils;
use rlink::utils::thread::get_runtime;

use crate::build_kafka_record;
use crate::source::handover::Handover;
use crate::state::OffsetMetadata;
use rlink::api::runtime::JobId;

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
    partition_offsets: Vec<OffsetMetadata>,
    handover: Handover,
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
        get_runtime().block_on(async {
            let mut kafka_consumer =
                KafkaConsumerThread::new(client_config, partition_offsets, handover_clone);
            kafka_consumer.run().await;
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
    partition_offsets: Vec<OffsetMetadata>,

    handover: Handover,
}

impl KafkaConsumerThread {
    pub fn new(
        client_config: ClientConfig,
        partition_offsets: Vec<OffsetMetadata>,
        handover: Handover,
    ) -> Self {
        KafkaConsumerThread {
            client_config,
            partition_offsets,
            handover,
        }
    }

    pub async fn run(&mut self) {
        let mut assignment = TopicPartitionList::new();
        for po in &self.partition_offsets {
            assignment.add_partition_offset(
                po.topic.as_str(),
                po.partition,
                Offset::from_raw(po.offset),
            )
        }

        // let group_id = format!("rlink{}", Uuid::new_v4());

        let consumer: StreamConsumer<DefaultConsumerContext> = self
            .client_config
            // .set("group.id", group_id.as_str())
            .create()
            .expect("Consumer creation failed");
        consumer
            .assign(&assignment)
            .expect("Can't subscribe to specified topics");

        info!(
            "create consumer success. config: {:?}, apply checkpoint offset: {:?}",
            self.client_config, self.partition_offsets
        );

        let mut message_stream = consumer.start();

        while let Some(message) = message_stream.next().await {
            match message {
                Ok(borrowed_message) => {
                    let topic = borrowed_message.topic();
                    let partition = borrowed_message.partition();
                    let offset = borrowed_message.offset();
                    let timestamp = borrowed_message.timestamp().to_millis().unwrap_or(0);
                    let key = borrowed_message.key().unwrap_or(&utils::EMPTY_SLICE);
                    let payload = borrowed_message.payload().unwrap_or(&utils::EMPTY_SLICE);

                    let mut record =
                        build_kafka_record(timestamp, key, payload, topic, partition, offset)
                            .expect("kafka message writer to Record error");

                    let mut loops = 0;
                    loop {
                        match self.handover.produce(record) {
                            Ok(_) => {
                                break;
                            }
                            Err(TrySendError::Full(r)) => {
                                record = r;

                                if loops == 5 {
                                    warn!("Handover produce `Full`");
                                } else if loops == 1000 {
                                    error!("Handover produce `Full` and try with 1000 times");
                                    loops = 0;
                                }
                                loops += 1;

                                tokio::time::delay_for(Duration::from_millis(100)).await;
                            }
                            Err(TrySendError::Disconnected(_r)) => {
                                panic!("handover produce `Disconnected`");
                            }
                        }
                    }
                }
                Err(e) => warn!("Kafka error: {}", e),
            }
        }
    }
}
