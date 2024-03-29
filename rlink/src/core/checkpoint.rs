use std::fmt::Debug;
use std::sync::Arc;

use crate::core::runtime::{CheckpointId, OperatorId, TaskId};
use crate::runtime::worker::WorkerTaskContext;

/// This struct provides a context in which user functions that use managed state metadata
#[derive(Clone, Debug)]
pub struct FunctionSnapshotContext {
    pub operator_id: OperatorId,
    pub task_id: TaskId,
    pub checkpoint_id: CheckpointId,
    pub completed_checkpoint_id: Option<CheckpointId>,

    pub(crate) task_context: Arc<WorkerTaskContext>,
}

impl FunctionSnapshotContext {
    pub(crate) fn new(
        operator_id: OperatorId,
        task_id: TaskId,
        checkpoint_id: CheckpointId,
        completed_checkpoint_id: Option<CheckpointId>,
        task_context: Arc<WorkerTaskContext>,
    ) -> Self {
        FunctionSnapshotContext {
            operator_id,
            task_id,
            checkpoint_id,
            completed_checkpoint_id,
            task_context,
        }
    }

    pub(crate) fn report(&self, ck: Checkpoint) -> Option<Checkpoint> {
        self.task_context.checkpoint_publish().report(ck)
    }
}

/// checkpoint handle
/// identify a checkpoint resource
/// eg: 1. checkpoint state and save to distribution file system, mark the file path as handle.
///     2. as mq system, mark the offset as handle
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckpointHandle {
    pub handle: String,
}

impl Default for CheckpointHandle {
    fn default() -> Self {
        Self {
            handle: "".to_string(),
        }
    }
}

/// descriptor a `Checkpoint`
/// use for network communication between `Coordinator` and `Worker`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub operator_id: OperatorId,
    pub task_id: TaskId,
    pub checkpoint_id: CheckpointId,
    pub completed_checkpoint_id: Option<CheckpointId>,
    pub handle: CheckpointHandle,
}

#[async_trait]
pub trait CheckpointFunction {
    fn consult_version(
        &mut self,
        context: &FunctionSnapshotContext,
        _handle: &Option<CheckpointHandle>,
    ) -> CheckpointId {
        context.checkpoint_id
    }

    /// trigger the method when a `operator` initialization
    async fn initialize_state(
        &mut self,
        _context: &FunctionSnapshotContext,
        _handle: &Option<CheckpointHandle>,
    );

    /// trigger the method when the `operator` operate a `Barrier` event
    async fn snapshot_state(
        &mut self,
        _context: &FunctionSnapshotContext,
    ) -> Option<CheckpointHandle>;
}
