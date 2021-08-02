use std::collections::HashMap;
use std::time::Duration;

use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::{ClientConfig, TopicPartitionList};

use crate::state::PartitionOffset;

pub trait OffsetLoader {
    fn load(&mut self, client_config: ClientConfig) -> KafkaResult<()>;
    fn begin_offset(&self) -> Option<&HashMap<String, Vec<PartitionOffset>>>;
    fn end_offset(&self) -> Option<&HashMap<String, Vec<PartitionOffset>>>;
}

pub struct DefaultOffsetLoader {
    begin_offset: Option<HashMap<String, Vec<PartitionOffset>>>,
    end_offset: Option<HashMap<String, Vec<PartitionOffset>>>,
}

impl DefaultOffsetLoader {
    pub fn new(
        begin_offset: Option<HashMap<String, Vec<PartitionOffset>>>,
        end_offset: Option<HashMap<String, Vec<PartitionOffset>>>,
    ) -> Self {
        DefaultOffsetLoader {
            begin_offset,
            end_offset,
        }
    }
}

impl OffsetLoader for DefaultOffsetLoader {
    fn load(&mut self, _client_config: ClientConfig) -> KafkaResult<()> {
        Ok(())
    }

    fn begin_offset(&self) -> Option<&HashMap<String, Vec<PartitionOffset>>> {
        self.begin_offset.as_ref()
    }

    fn end_offset(&self) -> Option<&HashMap<String, Vec<PartitionOffset>>> {
        self.end_offset.as_ref()
    }
}

pub struct TimestampOffsetLoader {
    begin_timestamp: u64,
    end_timestamp: Option<u64>,

    begin_offset: Option<HashMap<String, Vec<PartitionOffset>>>,
    end_offset: Option<HashMap<String, Vec<PartitionOffset>>>,
}

impl TimestampOffsetLoader {
    pub fn new(begin_timestamp: u64, end_timestamp: Option<u64>) -> Self {
        TimestampOffsetLoader {
            begin_timestamp,
            end_timestamp,
            begin_offset: None,
            end_offset: None,
        }
    }

    fn convert(
        &self,
        topic_partition_list: TopicPartitionList,
    ) -> HashMap<String, Vec<PartitionOffset>> {
        let mut po = HashMap::new();
        for elem in topic_partition_list.elements() {
            let p = po.entry(elem.topic().to_string()).or_insert(Vec::new());
            p.push(PartitionOffset {
                partition: elem.partition(),
                offset: elem.offset().to_raw().unwrap(),
            });
        }

        po
    }
}

impl OffsetLoader for TimestampOffsetLoader {
    fn load(&mut self, client_config: ClientConfig) -> KafkaResult<()> {
        let consumer: StreamConsumer<DefaultConsumerContext> = client_config.create()?;
        let timeout = Duration::from_secs(3);

        self.begin_offset = {
            let begin_topic_partition_list =
                consumer.offsets_for_timestamp(self.begin_timestamp as i64, timeout)?;
            Some(self.convert(begin_topic_partition_list))
        };

        self.end_offset = match self.end_timestamp {
            Some(end_timestamp) => {
                let end_topic_partition_list =
                    consumer.offsets_for_timestamp(end_timestamp as i64, timeout)?;
                Some(self.convert(end_topic_partition_list))
            }
            None => None,
        };

        Ok(())
    }

    fn begin_offset(&self) -> Option<&HashMap<String, Vec<PartitionOffset>>> {
        self.begin_offset.as_ref()
    }

    fn end_offset(&self) -> Option<&HashMap<String, Vec<PartitionOffset>>> {
        self.end_offset.as_ref()
    }
}
