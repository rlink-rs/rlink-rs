use std::fmt::Debug;

use crate::api::backend::{OperatorState, OperatorStateBackend};
use crate::runtime::ChainId;
use crate::storage::operator_state::empty_state::EmptyOperatorStateManager;

pub mod empty_state;

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
