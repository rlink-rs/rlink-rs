use std::fmt::Debug;

use crate::api::backend::{OperatorState, OperatorStateBackend};
use crate::api::runtime::{CheckpointId, JobId};
use crate::storage::operator_state::{OperatorStateManager, OperatorStateManagerWrap};

#[derive(Clone, Debug)]
pub struct FunctionSnapshotContext {
    pub job_id: JobId,
    pub task_number: u16,
    pub checkpoint_id: CheckpointId,
}

impl FunctionSnapshotContext {
    pub fn new(job_id: JobId, task_number: u16, checkpoint_id: CheckpointId) -> Self {
        FunctionSnapshotContext {
            job_id,
            task_number,
            checkpoint_id,
        }
    }

    pub fn create_state(
        &self,
        job_id: JobId,
        state: OperatorStateBackend,
        application_id: String,
        task_number: u16,
    ) -> Box<dyn OperatorState> {
        OperatorStateManagerWrap::new(job_id, state).create_state(application_id, task_number)
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

/// descriptor a `Checkpoint`
/// use for network communication between `Coordinator` and `Worker`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub job_id: JobId,
    pub task_num: u16,
    pub checkpoint_id: CheckpointId,
    pub handle: CheckpointHandle,
}

pub trait CheckpointedFunction
where
    Self: Debug,
{
    /// trigger the method when a `operator` initialization
    fn initialize_state(
        &mut self,
        context: &FunctionSnapshotContext,
        handle: &Option<CheckpointHandle>,
    );

    /// trigger the method when the `operator` operate a `Barrier` event
    fn snapshot_state(&mut self, context: &FunctionSnapshotContext) -> CheckpointHandle;
}
