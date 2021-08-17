use std::collections::HashMap;
use std::convert::TryFrom;

use rdkafka::ClientConfig;
use rlink::core::properties::Properties;

use crate::{
    KafkaOutputFormat, BOOTSTRAP_SERVERS, BUFFER_SIZE, KAFKA, SINK_CHANNEL_SIZE,
    SOURCE_CHANNEL_SIZE, TOPICS,
};

pub struct KafkaOutputFormatBuilder {
    conf_map: HashMap<String, String>,
    topics: Option<String>,
    buffer_size: Option<usize>,
}

impl KafkaOutputFormatBuilder {
    pub fn new(conf_map: HashMap<String, String>, topics: Option<String>) -> Self {
        KafkaOutputFormatBuilder {
            conf_map,
            topics,
            buffer_size: None,
        }
    }

    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = Some(size);
        self
    }

    pub fn build(self) -> KafkaOutputFormat {
        let mut client_config = ClientConfig::new();
        for (key, val) in &self.conf_map {
            client_config.set(key.as_str(), val.as_str());
        }

        let buffer_size = self.buffer_size.unwrap_or(SOURCE_CHANNEL_SIZE);

        KafkaOutputFormat::new(client_config, self.topics, buffer_size)
    }
}

impl TryFrom<Properties> for KafkaOutputFormatBuilder {
    type Error = anyhow::Error;

    fn try_from(properties: Properties) -> Result<Self, Self::Error> {
        let client_config = {
            let kafka_properties = properties.to_sub_properties(KAFKA);

            // check
            kafka_properties.get_string(BOOTSTRAP_SERVERS)?;

            kafka_properties.as_map().clone()
        };

        let topic = match properties.get_string(TOPICS) {
            Ok(topics) => {
                let topics: Vec<&str> = topics.trim().split(",").collect();
                if topics.len() != 1 {
                    panic!("only one topic support in kafka sink");
                }
                Some(topics[0].to_string())
            }
            Err(_e) => None,
        };

        let buffer_size = properties
            .get_usize(BUFFER_SIZE)
            .unwrap_or(SINK_CHANNEL_SIZE);

        let builder = KafkaOutputFormatBuilder::new(client_config, topic).buffer_size(buffer_size);

        Ok(builder)
    }
}
