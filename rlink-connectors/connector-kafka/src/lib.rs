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

use std::collections::HashMap;

use rdkafka::ClientConfig;
use rlink::core::element::FnSchema;
use rlink::core::element::Record;
use rlink::core::properties::Properties;

use crate::buffer_gen::kafka_message;
use crate::source::deserializer::{
    DefaultKafkaRecordDeserializer, DefaultKafkaRecordDeserializerBuilder,
    KafkaRecordDeserializerBuilder,
};
use crate::state::PartitionOffset;

pub const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
pub const TOPICS: &str = "topics";
pub const GROUP_ID: &str = "group.id";
pub const BUFFER_SIZE: &str = "buffer.size";

pub const KAFKA: &str = "kafka";
pub const REPLAY: &str = "replay";
pub const OFFSET_BEGIN: &str = "offset.begin";
pub const OFFSET_END: &str = "offset.end";

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

pub fn create_output_format_from_props(
    properties: &Properties,
    fn_name: Option<String>,
) -> anyhow::Result<KafkaOutputFormat> {
    let fn_name = fn_name.unwrap_or(OUTPUT_FORMAT_FN_NAME_DEFAULT.to_owned());
    let sink_properties = properties.get_sink_properties(fn_name.as_str());
    let kafka_properties = sink_properties.get_sub_properties(KAFKA);

    let mut client_config = ClientConfig::new();
    let bootstrap_servers = kafka_properties.get_string(BOOTSTRAP_SERVERS)?;
    client_config.set(BOOTSTRAP_SERVERS, bootstrap_servers);

    let topic = match kafka_properties.get_string(TOPICS) {
        Ok(topics) => {
            let topics: Vec<String> = serde_json::from_str(topics.as_str())
                .map_err(|_e| anyhow!("topics '{}' is not array", topics))?;
            Some(topics[0].clone())
        }
        Err(_e) => None,
    };

    let buffer_size = kafka_properties
        .get_usize(BUFFER_SIZE)
        .unwrap_or(SOURCE_CHANNEL_SIZE);

    let kafka_output_format = KafkaOutputFormat::new(client_config, topic, buffer_size);
    Ok(kafka_output_format)
}

pub fn create_input_format_from_props(
    properties: &Properties,
    fn_name: Option<String>,
    deserializer_builder: Option<Box<dyn KafkaRecordDeserializerBuilder>>,
) -> anyhow::Result<KafkaInputFormat> {
    let fn_name = fn_name.unwrap_or(INPUT_FORMAT_FN_NAME_DEFAULT.to_owned());
    let source_properties = properties.get_source_properties(fn_name.as_str());
    let kafka_properties = source_properties.get_sub_properties(KAFKA);

    let mut client_config = ClientConfig::new();
    let bootstrap_servers = kafka_properties.get_string(BOOTSTRAP_SERVERS)?;
    let group_id = kafka_properties.get_string(GROUP_ID)?;
    client_config.set(BOOTSTRAP_SERVERS, bootstrap_servers);
    client_config.set(GROUP_ID, group_id);

    let topics = kafka_properties.get_string(TOPICS)?;
    let topics: Vec<String> = serde_json::from_str(topics.as_str())
        .map_err(|_e| anyhow!("topics '{}' is not array", topics))?;

    let buffer_size = kafka_properties
        .get_usize(BUFFER_SIZE)
        .unwrap_or(SOURCE_CHANNEL_SIZE);

    let replay_properties = kafka_properties.get_sub_properties(REPLAY);
    let offset_range = if replay_properties.as_map().is_empty() {
        OffsetRange::None
    } else {
        let begin_offset = replay_properties.get_string(OFFSET_BEGIN)?;
        let begin_offset = parse_replay_offset(begin_offset.as_str())?;

        let mut end_offset = None;
        if let Ok(end_offset_str) = replay_properties.get_string(OFFSET_END) {
            let end_offset_map = parse_replay_offset(end_offset_str.as_str())?;
            end_offset = Some(end_offset_map);
        }

        OffsetRange::Direct {
            begin_offset,
            end_offset,
        }
    };

    let deserializer_builder = deserializer_builder.unwrap_or_else(|| {
        let deserializer_builder: Box<dyn KafkaRecordDeserializerBuilder> =
            Box::new(DefaultKafkaRecordDeserializerBuilder::<
                DefaultKafkaRecordDeserializer,
            >::new(FnSchema::from(
                &kafka_message::FIELD_METADATA,
            )));
        deserializer_builder
    });

    let kafka_input_format = KafkaInputFormat::new(
        client_config,
        topics,
        buffer_size,
        offset_range,
        deserializer_builder,
    );
    Ok(kafka_input_format)
}

fn parse_replay_offset(
    offset_config: &str,
) -> anyhow::Result<HashMap<String, Vec<PartitionOffset>>> {
    let offset: HashMap<String, Vec<i64>> = serde_json::from_str(offset_config)
        .map_err(|_e| anyhow!("offset config is illegal: {}", offset_config))?;

    let mut partition_offset = HashMap::new();
    for (topic, vec) in offset {
        let mut offset_vec = Vec::new();
        for (partition, value) in vec.iter().enumerate() {
            offset_vec.push(PartitionOffset::new(partition as i32, *value))
        }
        partition_offset.insert(topic, offset_vec);
    }

    Ok(partition_offset)
}

pub enum OffsetRange {
    None,
    Direct {
        begin_offset: HashMap<String, Vec<PartitionOffset>>,
        end_offset: Option<HashMap<String, Vec<PartitionOffset>>>,
    },
    Timestamp {
        begin_timestamp: HashMap<String, u64>,
        end_timestamp: Option<HashMap<String, u64>>,
    },
}

pub struct InputFormatBuilder {
    conf_map: HashMap<String, String>,
    topics: Vec<String>,
    buffer_size: Option<usize>,
    deserializer_builder: Option<Box<dyn KafkaRecordDeserializerBuilder>>,
    offset_range: OffsetRange,
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
            offset_range: OffsetRange::None,
        }
    }

    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = Some(size);
        self
    }

    pub fn offset_range(mut self, offset_range: OffsetRange) -> Self {
        self.offset_range = offset_range;
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
                >::new(FnSchema::from(
                    &kafka_message::FIELD_METADATA,
                )));

            deserializer_builder
        });

        KafkaInputFormat::new(
            client_config,
            self.topics,
            buffer_size,
            self.offset_range,
            deserializer_builder,
        )
    }
}
