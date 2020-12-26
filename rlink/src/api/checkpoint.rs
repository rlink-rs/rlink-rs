use crate::api::backend::{OperatorState, OperatorStateBackend};
use crate::runtime::ChainId;
use crate::storage::operator_state::{OperatorStateManager, OperatorStateManagerWrap};
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub struct FunctionSnapshotContext {
    pub chain_id: u32,
    pub task_number: u16,
    pub checkpoint_id: u64,
}

impl FunctionSnapshotContext {
    pub fn new(chain_id: u32, task_number: u16, checkpoint_id: u64) -> Self {
        FunctionSnapshotContext {
            chain_id,
            task_number,
            checkpoint_id,
        }
    }

    pub fn create_state(
        &self,
        chain_id: ChainId,
        state: OperatorStateBackend,
        job_id: String,
        task_number: u16,
    ) -> Box<dyn OperatorState> {
        OperatorStateManagerWrap::new(chain_id, state).create_state(job_id, task_number)
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
    pub chain_id: ChainId,
    pub task_num: u16,
    pub checkpoint_id: u64,
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
