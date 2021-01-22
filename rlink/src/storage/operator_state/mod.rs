use std::fmt::Debug;

use crate::api::backend::{OperatorState, OperatorStateBackend};
use crate::api::runtime::TaskId;
use crate::storage::operator_state::empty_state::EmptyOperatorStateManager;

pub mod empty_state;

pub(crate) trait TOperatorStateManager: Clone + Debug {
    fn create_state(&self, application_id: String, task_id: TaskId) -> Box<dyn OperatorState>;
}

#[derive(Clone, Debug)]
pub enum OperatorStateManager {
    EmptyOperatorStateManager(EmptyOperatorStateManager),
}

impl OperatorStateManager {
    pub(crate) fn new(_task_id: TaskId, state: OperatorStateBackend) -> Self {
        match state {
            OperatorStateBackend::None => {
                OperatorStateManager::EmptyOperatorStateManager(EmptyOperatorStateManager {})
            }
        }
    }
}

impl TOperatorStateManager for OperatorStateManager {
    fn create_state(&self, application_id: String, task_id: TaskId) -> Box<dyn OperatorState> {
        match self {
            OperatorStateManager::EmptyOperatorStateManager(manager) => {
                manager.create_state(application_id, task_id)
            }
        }
    }
}
