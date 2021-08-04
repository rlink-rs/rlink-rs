use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use rdkafka::Offset;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionOffset {
    pub(crate) partition: i32,
    pub(crate) offset: i64,
}

impl PartitionOffset {
    pub fn new(partition: i32, offset: i64) -> Self {
        Self { partition, offset }
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }
    pub fn offset(&self) -> i64 {
        self.offset
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartitionOffsets {
    pub(crate) partition_offsets: Vec<Option<PartitionOffset>>,
}

impl PartitionOffsets {
    pub fn new() -> Self {
        PartitionOffsets {
            partition_offsets: Vec::new(),
        }
    }

    pub fn update(&mut self, partition: i32, offset: i64) {
        let offset = Offset::Offset(offset).to_raw().unwrap();

        let partition_index = partition as usize;
        if partition_index >= self.partition_offsets.len() {
            for _ in self.partition_offsets.len()..(partition_index + 1) {
                self.partition_offsets.push(None)
            }
        }

        self.partition_offsets[partition_index] = Some(PartitionOffset { partition, offset });
    }
}

#[derive(Debug, Clone)]
pub struct KafkaSourceStateRecorder {
    partition_offsets: Arc<DashMap<String, PartitionOffsets>>,
}

impl KafkaSourceStateRecorder {
    pub fn new() -> Self {
        KafkaSourceStateRecorder {
            partition_offsets: Arc::new(DashMap::new()),
        }
    }

    pub fn update(&self, topic: &str, partition: i32, offset: i64) {
        if let Some(mut ref_val) = self.partition_offsets.get_mut(topic) {
            ref_val.update(partition, offset);
            return;
        } else {
            let mut po = PartitionOffsets::new();
            po.update(partition, offset);
            self.partition_offsets.insert(topic.to_string(), po);
        }
    }

    pub fn snapshot(&self) -> HashMap<String, PartitionOffsets> {
        let mut m = HashMap::new();
        self.partition_offsets.as_ref().iter().for_each(|ref_val| {
            m.insert(ref_val.key().to_string(), ref_val.value().clone());
        });
        m
    }

    pub fn get(&self, topic: &str, partition: i32) -> Option<PartitionOffset> {
        let kv_ref = self.partition_offsets.get(topic)?;
        let po = kv_ref.partition_offsets.get(partition as usize)?;
        po.clone()
    }
}
