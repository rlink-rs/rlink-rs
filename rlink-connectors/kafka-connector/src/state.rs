use std::sync::Arc;

use dashmap::DashMap;
use rdkafka::Offset;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PartitionMetadata {
    pub(crate) topic: String,
    pub(crate) partition: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OffsetMetadata {
    pub(crate) offset: i64,
}

#[derive(Debug, Clone)]
pub struct KafkaSourceStateCache {
    partition_offsets: Arc<DashMap<PartitionMetadata, OffsetMetadata>>,
}

impl KafkaSourceStateCache {
    pub fn new() -> Self {
        KafkaSourceStateCache {
            partition_offsets: Arc::new(DashMap::new()),
        }
    }

    pub fn update(&self, topic: String, partition: i32, offset: i64) {
        let key = PartitionMetadata { topic, partition };
        let val = OffsetMetadata {
            offset: Offset::Offset(offset).to_raw().unwrap(),
        };

        self.partition_offsets.insert(key, val);
    }

    pub fn snapshot(&self) -> DashMap<PartitionMetadata, OffsetMetadata> {
        let partition_offsets = &*self.partition_offsets;
        partition_offsets.clone()
    }

    pub fn get(
        &self,
        topic: String,
        partition: i32,
        default_offset: Offset,
    ) -> (PartitionMetadata, OffsetMetadata) {
        let key = PartitionMetadata {
            topic: topic.clone(),
            partition,
        };
        let val = match self.partition_offsets.get(&key) {
            Some(kv_ref) => kv_ref.clone(),
            None => OffsetMetadata {
                offset: default_offset.to_raw().unwrap(),
            },
        };

        (key, val)
    }
}
