use std::marker::PhantomData;

use rlink::api::element::Record;

use crate::build_kafka_record;

pub trait KafkaRecordDeserializer: Sync + Send {
    fn deserialize(
        &self,
        timestamp: i64,
        key: &[u8],
        payload: &[u8],
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Vec<Record>;
}

pub trait KafkaRecordDeserializerBuilder {
    fn build(&self) -> Box<dyn KafkaRecordDeserializer>;
}

#[derive(Default)]
pub struct DefaultKafkaRecordDeserializer {}

impl KafkaRecordDeserializer for DefaultKafkaRecordDeserializer {
    fn deserialize(
        &self,
        timestamp: i64,
        key: &[u8],
        payload: &[u8],
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Vec<Record> {
        let record = build_kafka_record(timestamp, key, payload, topic, partition, offset)
            .expect("kafka message writer to Record error");
        vec![record]
    }
}

pub struct DefaultKafkaRecordDeserializerBuilder<T>
where
    T: Default + KafkaRecordDeserializer + 'static,
{
    a: PhantomData<T>,
}

impl<T> DefaultKafkaRecordDeserializerBuilder<T>
where
    T: Default + KafkaRecordDeserializer + 'static,
{
    pub fn new() -> Self {
        DefaultKafkaRecordDeserializerBuilder { a: PhantomData }
    }
}

impl<T> KafkaRecordDeserializerBuilder for DefaultKafkaRecordDeserializerBuilder<T>
where
    T: Default + KafkaRecordDeserializer + 'static,
{
    fn build(&self) -> Box<dyn KafkaRecordDeserializer> {
        let t: Box<dyn KafkaRecordDeserializer> = Box::new(T::default());
        t
    }
}
