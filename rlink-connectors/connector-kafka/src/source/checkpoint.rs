use rlink::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use rlink::core::runtime::TaskId;

use crate::state::{KafkaSourceStateCache, OffsetMetadata, PartitionMetadata};

#[derive(Debug, Clone)]
pub struct KafkaCheckpointFunction {
    pub(crate) state_cache: Option<KafkaSourceStateCache>,
    pub(crate) application_id: String,
    pub(crate) task_id: TaskId,
}

impl KafkaCheckpointFunction {
    pub fn new(application_id: String, task_id: TaskId) -> Self {
        KafkaCheckpointFunction {
            state_cache: None,
            application_id,
            task_id,
        }
    }

    pub fn get_state(&mut self) -> &mut KafkaSourceStateCache {
        self.state_cache.as_mut().unwrap()
    }
}

impl CheckpointFunction for KafkaCheckpointFunction {
    fn initialize_state(
        &mut self,
        context: &FunctionSnapshotContext,
        handle: &Option<CheckpointHandle>,
    ) {
        self.state_cache = Some(KafkaSourceStateCache::new());
        info!("Checkpoint initialize, context: {:?}", context);

        if context.checkpoint_id.0 > 0 && handle.is_some() {
            let data = handle.as_ref().unwrap();

            let offsets: Vec<PartitionOffset> = serde_json::from_str(data.handle.as_str()).unwrap();
            for partition_offset in offsets {
                let PartitionOffset { partition, offset } = partition_offset;
                let PartitionMetadata { topic, partition } = partition;
                let OffsetMetadata { offset } = offset;

                let state_cache = self.state_cache.as_mut().unwrap();
                state_cache.update(topic, partition, offset);

                info!(
                    "load state value from checkpoint({:?}): {:?}",
                    context.checkpoint_id, data
                );
            }
        }
    }

    fn snapshot_state(&mut self, context: &FunctionSnapshotContext) -> Option<CheckpointHandle> {
        let offset_snapshot = self.state_cache.as_ref().unwrap().snapshot();
        debug!(
            "Checkpoint snapshot: {:?}, context: {:?}",
            offset_snapshot, context
        );
        let snapshot_serial: Vec<PartitionOffset> = offset_snapshot
            .into_iter()
            .map(|kv_ref| PartitionOffset {
                partition: kv_ref.0,
                offset: kv_ref.1,
            })
            .collect();

        let json = serde_json::to_string(&snapshot_serial).unwrap();

        Some(CheckpointHandle { handle: json })
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PartitionOffset {
    partition: PartitionMetadata,
    offset: OffsetMetadata,
}
