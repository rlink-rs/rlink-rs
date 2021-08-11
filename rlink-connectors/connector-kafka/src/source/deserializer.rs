use std::marker::PhantomData;

use rlink::core::element::{FnSchema, Record};

use crate::build_kafka_record;

pub trait KafkaRecordDeserializer: Sync + Send {
    fn deserialize(
        &mut self,
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
    fn schema(&self) -> FnSchema;
}

#[derive(Default)]
pub struct DefaultKafkaRecordDeserializer {}

impl KafkaRecordDeserializer for DefaultKafkaRecordDeserializer {
    fn deserialize(
        &mut self,
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
    schema: FnSchema,
}

impl<T> DefaultKafkaRecordDeserializerBuilder<T>
where
    T: Default + KafkaRecordDeserializer + 'static,
{
    pub fn new(schema: FnSchema) -> Self {
        DefaultKafkaRecordDeserializerBuilder {
            a: PhantomData,
            schema,
        }
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

    fn schema(&self) -> FnSchema {
        self.schema.clone()
    }
}
