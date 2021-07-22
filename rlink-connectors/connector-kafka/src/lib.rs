#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate rlink_derive;
#[macro_use]
extern crate anyhow;

pub mod sink;
pub mod source;

pub mod state;

pub use sink::output_format::KafkaOutputFormat;
pub use source::input_format::KafkaInputFormat;

use std::collections::HashMap;

use rdkafka::ClientConfig;
use rlink::core::element::BufferReader;
use rlink::core::element::Record;

use crate::source::deserializer::{
    DefaultKafkaRecordDeserializer, DefaultKafkaRecordDeserializerBuilder,
    KafkaRecordDeserializerBuilder,
};
use crate::state::PartitionOffset;

pub const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
pub const TOPICS: &str = "topics";
pub const GROUP_ID: &str = "group.id";

pub const SOURCE_CHANNEL_SIZE: usize = 50000;
pub const SINK_CHANNEL_SIZE: usize = 50000;

pub static KAFKA_DATA_TYPES: [u8; 6] = [
    // timestamp
    rlink::core::element::types::I64,
    // key
    rlink::core::element::types::BYTES,
    // payload
    rlink::core::element::types::BYTES,
    // topic
    rlink::core::element::types::BYTES,
    // partition
    rlink::core::element::types::I32,
    // offset
    rlink::core::element::types::I64,
];

pub fn build_kafka_record(
    timestamp: i64,
    key: &[u8],
    payload: &[u8],
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<Record, std::io::Error> {
    // 36 = 12(len(payload) + len(topic) + len(key)) +
    //      20(len(timestamp) + len(partition) + len(offset)) +
    //      4(place_holder)
    let capacity = payload.len() + topic.len() + key.len() + 36;
    let mut record = Record::with_capacity(capacity);
    let mut writer = record.as_writer(&KAFKA_DATA_TYPES);
    writer.set_i64(timestamp)?;
    writer.set_bytes(key)?;
    writer.set_bytes(payload)?;
    writer.set_str(topic)?;
    writer.set_i32(partition)?;
    writer.set_i64(offset)?;

    Ok(record)
}

pub struct KafkaRecord<'a, 'b> {
    reader: BufferReader<'a, 'b>,
}

impl<'a, 'b> KafkaRecord<'a, 'b> {
    pub fn new(record: &'a mut Record) -> Self {
        let reader = record.as_reader(&KAFKA_DATA_TYPES);
        KafkaRecord { reader }
    }

    pub fn get_kafka_timestamp(&self) -> Result<i64, std::io::Error> {
        self.reader.get_i64(0)
    }

    pub fn get_kafka_key(&self) -> Result<&[u8], std::io::Error> {
        self.reader.get_bytes(1)
    }

    pub fn get_kafka_payload(&self) -> Result<&[u8], std::io::Error> {
        self.reader.get_bytes(2)
    }

    pub fn get_kafka_topic(&self) -> Result<&str, std::io::Error> {
        self.reader.get_str(3)
    }

    pub fn get_kafka_partition(&self) -> Result<i32, std::io::Error> {
        self.reader.get_i32(4)
    }

    pub fn get_kafka_offset(&self) -> Result<i64, std::io::Error> {
        self.reader.get_i64(5)
    }
}

pub fn create_output_format(
    conf_map: HashMap<String, String>,
    topic: Option<String>,
    buffer_size: Option<usize>,
) -> KafkaOutputFormat {
    let mut client_config = ClientConfig::new();
    for (key, val) in conf_map {
        client_config.set(key.as_str(), val.as_str());
    }

    KafkaOutputFormat::new(
        client_config,
        topic,
        buffer_size.unwrap_or(SINK_CHANNEL_SIZE),
    )
}

pub struct InputFormatBuilder {
    conf_map: HashMap<String, String>,
    topics: Vec<String>,
    buffer_size: Option<usize>,
    deserializer_builder: Option<Box<dyn KafkaRecordDeserializerBuilder>>,
    begin_offset: Option<HashMap<String, Vec<PartitionOffset>>>,
    end_offset: Option<HashMap<String, Vec<PartitionOffset>>>,
}

impl InputFormatBuilder {
    pub fn new(
        conf_map: HashMap<String, String>,
        topics: Vec<String>,
        deserializer_builder: Option<Box<dyn KafkaRecordDeserializerBuilder>>,
    ) -> Self {
        InputFormatBuilder {
            conf_map,
            topics,
            buffer_size: None,
            deserializer_builder,
            begin_offset: None,
            end_offset: None,
        }
    }

    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = Some(size);
        self
    }

    pub fn begin_offset(mut self, begin_offset: HashMap<String, Vec<PartitionOffset>>) -> Self {
        self.begin_offset = Some(begin_offset);
        self
    }

    pub fn end_offset(mut self, end_offset: HashMap<String, Vec<PartitionOffset>>) -> Self {
        self.end_offset = Some(end_offset);
        self
    }

    pub fn build(self) -> KafkaInputFormat {
        let mut client_config = ClientConfig::new();
        for (key, val) in &self.conf_map {
            client_config.set(key.as_str(), val.as_str());
        }

        let buffer_size = self.buffer_size.unwrap_or(SOURCE_CHANNEL_SIZE);

        let deserializer_builder = self.deserializer_builder.unwrap_or_else(|| {
            let deserializer_builder: Box<dyn KafkaRecordDeserializerBuilder> =
                Box::new(DefaultKafkaRecordDeserializerBuilder::<
                    DefaultKafkaRecordDeserializer,
                >::new());

            deserializer_builder
        });

        KafkaInputFormat::new(
            client_config,
            self.topics,
            buffer_size,
            self.begin_offset,
            self.end_offset,
            deserializer_builder,
        )
    }
}
