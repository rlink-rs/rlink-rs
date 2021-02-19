use std::fmt::Debug;

use crate::api::runtime::{CheckpointId, OperatorId, TaskId};

#[derive(Clone, Debug)]
pub struct FunctionSnapshotContext {
    pub operator_id: OperatorId,
    pub task_id: TaskId,
    pub checkpoint_id: CheckpointId,
}

impl FunctionSnapshotContext {
    pub fn new(operator_id: OperatorId, task_id: TaskId, checkpoint_id: CheckpointId) -> Self {
        FunctionSnapshotContext {
            operator_id,
            task_id,
            checkpoint_id,
        }
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
    pub handle: CheckpointHandle,
}

pub trait CheckpointFunction {
    fn consult_version(
        &mut self,
        context: &FunctionSnapshotContext,
        _handle: &Option<CheckpointHandle>,
    ) -> CheckpointId {
        context.checkpoint_id
    }

    /// trigger the method when a `operator` initialization
    fn initialize_state(
        &mut self,
        _context: &FunctionSnapshotContext,
        _handle: &Option<CheckpointHandle>,
    ) {
    }

    /// trigger the method when the `operator` operate a `Barrier` event
    fn snapshot_state(&mut self, _context: &FunctionSnapshotContext) -> Option<CheckpointHandle> {
        None
    }
}
