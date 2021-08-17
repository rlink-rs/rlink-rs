use std::collections::HashMap;
use std::convert::TryFrom;

use rdkafka::ClientConfig;
use rlink::core::element::FnSchema;
use rlink::core::properties::Properties;

use crate::buffer_gen::kafka_message;
use crate::source::deserializer::{
    DefaultKafkaRecordDeserializer, DefaultKafkaRecordDeserializerBuilder,
    KafkaRecordDeserializerBuilder,
};
use crate::source::offset_range::OffsetRange;
use crate::{
    KafkaInputFormat, BOOTSTRAP_SERVERS, BUFFER_SIZE, GROUP_ID, KAFKA, OFFSET, SOURCE_CHANNEL_SIZE,
    TOPICS,
};

pub struct KafkaInputFormatBuilder {
    parallelism: u16,
    conf_map: HashMap<String, String>,
    topics: Vec<String>,
    buffer_size: Option<usize>,
    offset_range: OffsetRange,
}

impl KafkaInputFormatBuilder {
    pub fn new(conf_map: HashMap<String, String>, topics: Vec<String>, parallelism: u16) -> Self {
        KafkaInputFormatBuilder {
            parallelism,
            conf_map,
            topics,
            buffer_size: None,
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

    pub fn build(
        self,
        deserializer_builder: Option<Box<dyn KafkaRecordDeserializerBuilder>>,
    ) -> KafkaInputFormat {
        let mut client_config = ClientConfig::new();
        for (key, val) in &self.conf_map {
            client_config.set(key.as_str(), val.as_str());
        }

        let buffer_size = self.buffer_size.unwrap_or(SOURCE_CHANNEL_SIZE);

        let deserializer_builder = deserializer_builder.unwrap_or_else(|| {
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
            self.parallelism,
        )
    }
}

impl TryFrom<Properties> for KafkaInputFormatBuilder {
    type Error = anyhow::Error;

    fn try_from(properties: Properties) -> Result<Self, Self::Error> {
        let parallelism = properties.get_u16("parallelism")?;

        let client_config = {
            let kafka_properties = properties.to_sub_properties(KAFKA);

            // check
            kafka_properties.get_string(BOOTSTRAP_SERVERS)?;
            kafka_properties.get_string(GROUP_ID)?;

            kafka_properties.as_map().clone()
        };

        let topics = properties.get_string(TOPICS)?;
        let topics: Vec<String> = topics.trim().split(",").map(|x| x.to_string()).collect();
        if topics.len() == 0 {
            return Err(anyhow!("`topics` not found"));
        }

        let buffer_size = properties
            .get_usize(BUFFER_SIZE)
            .unwrap_or(SOURCE_CHANNEL_SIZE);

        let offset_properties = properties.to_sub_properties(OFFSET);
        let offset_range = OffsetRange::try_from(offset_properties)?;

        let builder = KafkaInputFormatBuilder::new(client_config, topics, parallelism)
            .buffer_size(buffer_size)
            .offset_range(offset_range);

        Ok(builder)
    }
}