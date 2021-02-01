use rlink::api::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use rlink::api::runtime::TaskId;

use crate::state::{KafkaSourceStateCache, OffsetMetadata};

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

            let offsets: Vec<OffsetMetadata> = serde_json::from_str(data.handle.as_str()).unwrap();
            for offset in offsets {
                let OffsetMetadata {
                    topic,
                    partition,
                    offset,
                } = offset;
                let state_cache = self.state_cache.as_mut().unwrap();
                state_cache.update(topic, partition, offset);

                info!(
                    "load state value from checkpoint({:?}): {:?}",
                    context.checkpoint_id, data
                );
            }
        }
    }

    fn snapshot_state(&mut self, context: &FunctionSnapshotContext) -> CheckpointHandle {
        let offset_snapshot = self.state_cache.as_ref().unwrap().snapshot();
        debug!(
            "Checkpoint snapshot: {:?}, context: {:?}",
            offset_snapshot, context
        );
        let snapshot_serial: Vec<OffsetMetadata> =
            offset_snapshot.into_iter().map(|kv_ref| kv_ref.1).collect();

        let json = serde_json::to_string(&snapshot_serial).unwrap();

        CheckpointHandle { handle: json }
    }
}
