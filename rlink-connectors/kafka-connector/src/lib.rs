#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate rlink_derive;

pub mod sink;
pub mod source;

pub(crate) mod state;

pub use sink::output_format::KafkaOutputFormat;
pub use source::input_format::KafkaInputFormat;

use std::collections::HashMap;

use rdkafka::ClientConfig;
use rlink::api::element::BufferReader;
use rlink::api::element::Record;

pub const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
pub const TOPICS: &str = "topics";
pub const GROUP_ID: &str = "group.id";

pub const SOURCE_CHANNEL_SIZE: usize = 100000;
pub const SINK_CHANNEL_SIZE: usize = 50000;

pub static KAFKA_DATA_TYPES: [u8; 6] = [
    // timestamp
    rlink::api::element::types::I64,
    // key
    rlink::api::element::types::BYTES,
    // payload
    rlink::api::element::types::BYTES,
    // topic
    rlink::api::element::types::BYTES,
    // partition
    rlink::api::element::types::I32,
    // offset
    rlink::api::element::types::I64,
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
    let mut writer = record.get_writer(&KAFKA_DATA_TYPES);
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
        let reader = record.get_reader(&KAFKA_DATA_TYPES);
        KafkaRecord { reader }
    }

    pub fn get_kafka_timestamp(&mut self) -> Result<i64, std::io::Error> {
        self.reader.get_i64(0)
    }

    pub fn get_kafka_key(&mut self) -> Result<&[u8], std::io::Error> {
        self.reader.get_bytes(1)
    }

    pub fn get_kafka_payload(&mut self) -> Result<&[u8], std::io::Error> {
        self.reader.get_bytes(2)
    }

    pub fn get_kafka_topic(&mut self) -> Result<String, std::io::Error> {
        self.reader.get_str(3)
    }

    pub fn get_kafka_partition(&mut self) -> Result<i32, std::io::Error> {
        self.reader.get_i32(4)
    }

    pub fn get_kafka_offset(&mut self) -> Result<i64, std::io::Error> {
        self.reader.get_i64(5)
    }
}

pub fn field(field_name: &str) -> String {
    format!("KAFKA_SOURCE.{}", field_name)
}

// pub fn create_input_format_from_prop(properties: &Properties) -> KafkaInputFormat {
//     let mut conf_map = HashMap::new();
//     conf_map.insert(
//         BOOTSTRAP_SERVERS.to_string(),
//         properties
//             .get_string(field(BOOTSTRAP_SERVERS).as_str())
//             .unwrap(),
//     );
//
//     let topics = properties.get_string(field(TOPICS).as_str()).unwrap();
//     let topics: Vec<&str> = topics.split(",").collect();
//     let topics = topics.iter().map(|topic| topic.to_string()).collect();
//
//     create_input_format(conf_map, topics)
// }

pub fn create_input_format(
    conf_map: HashMap<String, String>,
    topics: Vec<String>,
    buffer_size: Option<usize>,
) -> KafkaInputFormat {
    let mut client_config = ClientConfig::new();
    for (key, val) in conf_map {
        client_config.set(key.as_str(), val.as_str());
    }

    KafkaInputFormat::new(
        client_config,
        topics,
        buffer_size.unwrap_or(SOURCE_CHANNEL_SIZE),
    )
}

pub fn create_output_format(
    conf_map: HashMap<String, String>,
    topic: String,
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
