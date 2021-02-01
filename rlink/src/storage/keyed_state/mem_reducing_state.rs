use std::collections::BTreeMap;

use crate::api::element::Record;
use crate::storage::keyed_state::mem_storage::remove_drop_window;
use crate::storage::keyed_state::{StateIterator, StateKey, TReducingState};

// type RecordBuildHasher = std::hash::BuildHasherDefault<RecordHasher>;

#[derive(Clone, Debug)]
pub struct MemoryReducingState {
    state_key: StateKey,
    kv: BTreeMap<Record, Record>,
}

impl MemoryReducingState {
    pub fn new(state_key: &StateKey) -> Self {
        debug!("create memory state {:?}", state_key);
        MemoryReducingState {
            state_key: state_key.clone(),
            kv: BTreeMap::new(),
        }
    }

    pub fn from(state_key: &StateKey) -> Option<MemoryReducingState> {
        let state = remove_drop_window(
            state_key.job_id,
            state_key.task_number,
            state_key.window.clone(),
        );
        if state.is_some() {
            debug!("remove state {:?}", state_key);
        } else {
            error!("can not found state {:?}", state_key);
        }

        state
    }
}

impl TReducingState for MemoryReducingState {
    fn get_mut(&mut self, key: &Record) -> Option<&mut Record> {
        self.kv.get_mut(key)
    }

    fn insert(&mut self, key: Record, val: Record) {
        self.kv.insert(key, val);
    }

    fn flush(&mut self) {}

    fn snapshot(&mut self) {}

    fn close(self) {}

    fn destroy(self) {}

    fn iter(self) -> StateIterator {
        StateIterator::BTreeMap(self.state_key.window, self.kv.into_iter())
    }

    fn len(&self) -> usize {
        self.kv.len()
    }
}
