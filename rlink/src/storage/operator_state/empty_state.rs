use crate::api::backend::{OperatorState, StateValue};
use crate::runtime::CheckpointId;
use crate::storage::operator_state::OperatorStateManager;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct EmptyOperatorState {}

impl OperatorState for EmptyOperatorState {
    fn update(&mut self, _checkpoint_id: u64, _values: Vec<String>) {}

    fn snapshot(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn load_latest(
        &self,
        _checkpoint_id: CheckpointId,
    ) -> std::io::Result<HashMap<u16, StateValue>> {
        Ok(HashMap::new())
    }
}

#[derive(Clone, Debug)]
pub struct EmptyOperatorStateManager {}

impl OperatorStateManager for EmptyOperatorStateManager {
    fn create_state(&self, _job_id: String, _task_number: u16) -> Box<dyn OperatorState> {
        let state = EmptyOperatorState {};
        let state: Box<dyn OperatorState> = Box::new(state);
        state
    }
}
