use std::borrow::BorrowMut;
use std::collections::HashMap;

use crate::api::element::{Barrier, Record};
use crate::api::runtime::JobId;
use crate::api::window::Window;
use crate::storage::keyed_state::mem_reducing_state::MemoryReducingState;
use crate::storage::keyed_state::mem_storage::{append_drop_window, StorageKey};
use crate::storage::keyed_state::{StateKey, TReducingState, TWindowState};

#[derive(Clone, Debug)]
pub struct MemoryWindowState {
    application_id: String,
    job_id: JobId,
    task_number: u16,

    windows: HashMap<Window, MemoryReducingState>,
    suggest_state_capacity: usize,
}

impl MemoryWindowState {
    pub fn new(application_id: String, job_id: JobId, task_number: u16) -> Self {
        MemoryWindowState {
            application_id,
            job_id,
            task_number,
            windows: HashMap::new(),
            suggest_state_capacity: 512,
        }
    }

    fn merge_value<F>(&mut self, window: &Window, key: Record, record: &mut Record, reduce_fun: F)
    where
        F: Fn(Option<&mut Record>, &mut Record) -> Record,
    {
        match self.windows.get_mut(window) {
            Some(state) => {
                let state_record = state.get_mut(&key);
                let new_val = reduce_fun(state_record, record);
                state.insert(key, new_val);
            }
            None => {
                let state_key = StateKey::new(window.clone(), self.job_id, self.task_number);
                let mut state = MemoryReducingState::new(&state_key, self.suggest_state_capacity);

                let new_val = reduce_fun(None, record);
                state.insert(key, new_val);

                self.windows.insert(window.clone(), state);
            }
        }
    }
}

impl TWindowState for MemoryWindowState {
    fn windows(&self) -> Vec<Window> {
        let mut windows = Vec::new();
        for entry in &self.windows {
            windows.push(entry.0.clone())
        }

        windows
    }

    fn merge<F>(&mut self, key: Record, mut record: Record, reduce_fun: F)
    where
        F: Fn(Option<&mut Record>, &mut Record) -> Record,
    {
        let windows = record.get_location_windows();

        if windows.len() == 1 {
            let window = &windows[0].clone();
            self.merge_value(window, key, record.borrow_mut(), reduce_fun);
        } else {
            for window in &windows.clone() {
                self.merge_value(window, key.clone(), record.borrow_mut(), |value, record| {
                    reduce_fun(value, record)
                })
            }
        }
    }

    fn drop_window(&mut self, window: &Window) {
        match self.windows.remove(&window) {
            Some(state) => {
                let len = state.len() as f32;
                self.suggest_state_capacity = (len * 1.1f32) as usize;

                let state_key = StorageKey::new(self.job_id, self.task_number);
                append_drop_window(state_key, window.clone(), state);
            }
            None => {}
        };
    }

    fn snapshot(&mut self, _barrier: Barrier) {}
}

#[cfg(test)]
mod tests {

    #[test]
    pub fn dash_map_test() {
        let map = dashmap::DashMap::new();
        map.insert("a".to_string(), 1);

        assert_eq!(map.len(), 1);
        assert_eq!(map.get("a").unwrap().value().clone(), 1);
    }
}
