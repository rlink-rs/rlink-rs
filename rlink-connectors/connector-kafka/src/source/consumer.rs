use futures::StreamExt;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use rlink::channel::utils::handover::Handover;
use rlink::core::runtime::JobId;
use rlink::utils;
use rlink::utils::thread::async_runtime;

use crate::source::deserializer::KafkaRecordDeserializer;
use crate::source::{empty_record, ConsumerRecord};

#[derive(Debug, Clone)]
pub(crate) struct ConsumerRange {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) begin_offset: i64,
    pub(crate) end_offset: Option<i64>,
}

pub(crate) fn create_kafka_consumer(
    job_id: JobId,
    task_number: u16,
    client_config: ClientConfig,
    consumer_ranges: ConsumerRange,
    handover: Handover<ConsumerRecord>,
    deserializer: Box<dyn KafkaRecordDeserializer>,
) {
    utils::thread::spawn("kafka-source-block", move || {
        async_runtime("kafka_source").block_on(async {
            let mut kafka_consumer = KafkaConsumerThread::new(
                job_id,
                task_number,
                client_config,
                consumer_ranges,
                handover,
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
}

pub(crate) struct KafkaConsumerThread {
    job_id: JobId,
    task_number: u16,

    client_config: ClientConfig,
    consumer_ranges: ConsumerRange,
    with_end_consumer_ranges: bool,

    handover: Handover<ConsumerRecord>,
    deserializer: Box<dyn KafkaRecordDeserializer>,
}

impl KafkaConsumerThread {
    pub fn new(
        job_id: JobId,
        task_number: u16,
        client_config: ClientConfig,
        consumer_ranges: ConsumerRange,
        handover: Handover<ConsumerRecord>,
        deserializer: Box<dyn KafkaRecordDeserializer>,
    ) -> Self {
        let with_end_consumer_ranges = consumer_ranges.end_offset.is_some();
        KafkaConsumerThread {
            job_id,
            task_number,
            client_config,
            consumer_ranges,
            with_end_consumer_ranges,
            handover,
            deserializer,
        }
    }

    fn end_check(&self, topic: &str, partition: i32, offset: i64) -> bool {
        if !self.with_end_consumer_ranges {
            return false;
        }

        if self.consumer_ranges.partition == partition
            && self.consumer_ranges.end_offset.unwrap() < offset
            && self.consumer_ranges.topic.eq(topic)
        {
            return true;
        }

        false
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        let mut assignment = TopicPartitionList::new();
        assignment
            .add_partition_offset(
                self.consumer_ranges.topic.as_str(),
                self.consumer_ranges.partition,
                Offset::from_raw(self.consumer_ranges.begin_offset),
            )
            .unwrap();

        // let group_id = format!("rlink{}", Uuid::new_v4());
        self.client_config
            .get("group.id")
            .ok_or(anyhow!("`group.id` not found in kafka consumer config"))?;

        let consumer: StreamConsumer<DefaultConsumerContext> = self.client_config.create()?;
        consumer.assign(&assignment)?;

        info!(
            "create consumer success. config: {:?}, assign: {:?}, offset range: {:?},job_id: {}, task_num: {}",
            self.client_config, assignment, self.consumer_ranges, *self.job_id, self.task_number
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

                    if self.end_check(topic, partition, offset) {
                        self.handover
                            .produce(ConsumerRecord::new(empty_record(), 0))
                            .expect("kafka consumer handover `Disconnected`");
                        info!(
                            "kafka end offset reached. job_id: {}, task_num: {}",
                            *self.job_id, self.task_number
                        );
                        break;
                    }

                    let records = self
                        .deserializer
                        .deserialize(timestamp, key, payload, topic, partition, offset);

                    for record in records {
                        self.handover
                            .produce(ConsumerRecord::new(record, offset))
                            .expect("kafka consumer handover `Disconnected`");
                    }
                }
                Err(e) => warn!(
                    "Kafka consume error. job_id: {}, task_num: {}, error: {}",
                    *self.job_id, self.task_number, e
                ),
            }
        }

        Ok(())
    }
}
