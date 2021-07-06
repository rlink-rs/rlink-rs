use std::collections::HashMap;

use rlink::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use rlink::core::runtime::TaskId;

use crate::state::{KafkaSourceStateRecorder, PartitionOffset, PartitionOffsets};

#[derive(Debug, Clone)]
pub struct KafkaCheckpointFunction {
    pub(crate) state_recorder: Option<KafkaSourceStateRecorder>,
    pub(crate) application_id: String,
    pub(crate) task_id: TaskId,
}

impl KafkaCheckpointFunction {
    pub fn new(application_id: String, task_id: TaskId) -> Self {
        KafkaCheckpointFunction {
            state_recorder: None,
            application_id,
            task_id,
        }
    }

    pub fn get_state(&mut self) -> &mut KafkaSourceStateRecorder {
        self.state_recorder.as_mut().unwrap()
    }
}

impl CheckpointFunction for KafkaCheckpointFunction {
    fn initialize_state(
        &mut self,
        context: &FunctionSnapshotContext,
        handle: &Option<CheckpointHandle>,
    ) {
        self.state_recorder = Some(KafkaSourceStateRecorder::new());
        info!("Checkpoint initialize, context: {:?}", context);

        if context.checkpoint_id.0 > 0 && handle.is_some() {
            let data = handle.as_ref().unwrap();

            let offsets: HashMap<String, PartitionOffsets> =
                serde_json::from_str(data.handle.as_str()).unwrap();

            for (topic, partition_offsets) in offsets {
                for partition_offset in partition_offsets.partition_offsets {
                    if let Some(partition_offset) = partition_offset {
                        let PartitionOffset { partition, offset } = partition_offset;

                        let state_cache = self.state_recorder.as_mut().unwrap();
                        state_cache.update(topic.as_str(), partition, offset);

                        info!(
                            "load state value from checkpoint({:?}): {:?}",
                            context.checkpoint_id, data
                        );
                    }
                }
            }
        }
    }

    fn snapshot_state(&mut self, context: &FunctionSnapshotContext) -> Option<CheckpointHandle> {
        let offset_snapshot = self.state_recorder.as_ref().unwrap().snapshot();
        debug!(
            "Checkpoint snapshot: {:?}, context: {:?}",
            offset_snapshot, context
        );

        let json = serde_json::to_string(&offset_snapshot).unwrap();

        Some(CheckpointHandle { handle: json })
    }
}
