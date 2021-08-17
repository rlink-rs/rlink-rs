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

pub mod buffer_gen {
    include!(concat!(env!("OUT_DIR"), "/buffer_gen/mod.rs"));
}

pub use sink::output_format::KafkaOutputFormat;
pub use source::input_format::KafkaInputFormat;

use rlink::core::element::Record;

use crate::buffer_gen::kafka_message;

pub const KAFKA: &str = "kafka";
pub const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
pub const GROUP_ID: &str = "group.id";

pub const TOPICS: &str = "topics";
pub const BUFFER_SIZE: &str = "buffer.size";

pub const OFFSET: &str = "offset";
pub const OFFSET_TYPE: &str = "type";
pub const OFFSET_BEGIN: &str = "begin";
pub const OFFSET_END: &str = "end";

pub const INPUT_FORMAT_FN_NAME_DEFAULT: &str = "KafkaInputFormat";
pub const OUTPUT_FORMAT_FN_NAME_DEFAULT: &str = "KafkaOutputFormat";

pub const SOURCE_CHANNEL_SIZE: usize = 50000;
pub const SINK_CHANNEL_SIZE: usize = 50000;

pub fn build_kafka_record(
    timestamp: i64,
    key: &[u8],
    payload: &[u8],
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<Record, std::io::Error> {
    let message = kafka_message::Entity {
        timestamp,
        key,
        payload,
        topic,
        partition,
        offset,
    };

    // 36 = 12(len(payload) + len(topic) + len(key)) +
    //      20(len(timestamp) + len(partition) + len(offset)) +
    //      4(place_holder)
    let capacity = payload.len() + topic.len() + key.len() + 36;
    let mut record = Record::with_capacity(capacity);

    message.to_buffer(record.as_buffer()).unwrap();

    Ok(record)
}
