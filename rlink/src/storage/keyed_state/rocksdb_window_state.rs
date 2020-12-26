use crate::api::element::{Barrier, Record};
use crate::api::window::{Window, WindowWrap};
use crate::storage::keyed_state::rocksdb_reducing_state::RocksDBReducingState;
use crate::storage::keyed_state::{ReducingState, StateKey, WindowState};
use std::borrow::BorrowMut;
use std::collections::HashMap;

#[derive(Debug)]
pub struct RocksDBWindowState {
    job_id: String,
    chain_id: u32,
    task_number: u16,
    path: String,
    reducing_states: HashMap<StateKey, RocksDBReducingState>,
    max_drop_window_timestamp: u64,
}

impl RocksDBWindowState {
    pub fn new(job_id: String, chain_id: u32, task_number: u16, path: String) -> Self {
        RocksDBWindowState {
            job_id,
            chain_id,
            task_number,
            path,
            reducing_states: HashMap::new(),
            max_drop_window_timestamp: 0,
        }
    }

    #[inline]
    fn create_state_key(&self, window: WindowWrap) -> StateKey {
        StateKey::new(window, self.chain_id, self.task_number)
    }
}

impl WindowState for RocksDBWindowState {
    fn windows(&self) -> Vec<WindowWrap> {
        self.reducing_states
            .keys()
            .map(|state_key| state_key.window.clone())
            .collect()
    }

    fn merge<F>(
        &mut self,
        windows: &Vec<WindowWrap>,
        key: Record,
        mut record: Record,
        reduce_fun: F,
    ) where
        F: Fn(Option<Record>, &mut Record) -> Record,
    {
        for window in windows {
            let state_key = self.create_state_key(window.clone());
            let reducing_state = match self.reducing_states.get_mut(&state_key) {
                Some(state) => state,
                None => {
                    if window.max_timestamp() <= self.max_drop_window_timestamp {
                        warn!(
                            "window not exist, timestamp={} too low, Record timestamp={}",
                            window.max_timestamp(),
                            &record.timestamp,
                        );
                        continue;
                    }

                    let state = RocksDBReducingState::new(&state_key, self.path.as_str(), false);
                    self.reducing_states.insert(state_key.clone(), state);
                    self.reducing_states.get_mut(&state_key).unwrap()
                }
            };

            let rt = reducing_state.get(&key);
            let new_value = reduce_fun(rt, record.borrow_mut());
            reducing_state.insert(key.clone(), new_value);
            // match reducing_state.get(&key) {
            //     Some(origin_value) => {
            //         let new_value = reduce_fun(&origin_value, &val);
            //         reducing_state.insert(key.clone(), new_value);
            //     }
            //     None => {
            //         reducing_state.insert(key.clone(), val.clone());
            //     }
            // }
        }
    }

    fn drop_window(&mut self, window: &WindowWrap) {
        let state_key = self.create_state_key(window.clone());
        let state = self.reducing_states.remove(&state_key);
        state
            .map(|mut state| {
                if self.max_drop_window_timestamp < window.max_timestamp() {
                    self.max_drop_window_timestamp = window.max_timestamp();
                }

                state.flush();
                state.close();
            })
            .unwrap();
    }

    fn snapshot(&mut self, _barrier: Barrier) {
        for (_, state) in &mut self.reducing_states {
            state.snapshot()
        }
    }
}
