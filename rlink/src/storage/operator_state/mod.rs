use std::fmt::Debug;

use crate::api::backend::{OperatorState, OperatorStateBackend};
use crate::runtime::ChainId;
use crate::storage::operator_state::empty_state::EmptyOperatorStateManager;

pub mod empty_state;

// #[derive(Clone, Debug)]
// pub struct StateName {
//     pub(crate) job_id: String,
//     pub(crate) chain_id: ChainId,
//     pub(crate) task_number: u16,
//     pub(crate) checkpoint_id: CheckpointId,
//     pub(crate) progress: bool,
// }
//
// impl StateName {
//     pub fn new(job_id: String, chain_id: u32, task_number: u16, checkpoint_id: u64) -> Self {
//         StateName {
//             job_id,
//             chain_id,
//             task_number,
//             checkpoint_id,
//             progress: false,
//         }
//     }
// }

pub(crate) trait OperatorStateManager: Clone + Debug {
    fn create_state(&self, job_id: String, task_number: u16) -> Box<dyn OperatorState>;
}

#[derive(Clone, Debug)]
pub enum OperatorStateManagerWrap {
    EmptyOperatorStateManager(EmptyOperatorStateManager),
}

impl OperatorStateManagerWrap {
    pub(crate) fn new(_chain_id: ChainId, state: OperatorStateBackend) -> Self {
        match state {
            OperatorStateBackend::None => {
                OperatorStateManagerWrap::EmptyOperatorStateManager(EmptyOperatorStateManager {})
            }
        }
    }
}

impl OperatorStateManager for OperatorStateManagerWrap {
    fn create_state(&self, job_id: String, task_number: u16) -> Box<dyn OperatorState> {
        match self {
            OperatorStateManagerWrap::EmptyOperatorStateManager(manager) => {
                manager.create_state(job_id, task_number)
            }
        }
    }
}
