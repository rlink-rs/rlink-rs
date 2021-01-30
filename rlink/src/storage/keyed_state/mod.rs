use std::collections::btree_map::IntoIter;
use std::fmt::Debug;

use crate::api::backend::KeyedStateBackend;
use crate::api::element::{Barrier, Record};
use crate::api::runtime::JobId;
use crate::api::window::Window;
use crate::storage::keyed_state::mem_reducing_state::MemoryReducingState;
use crate::storage::keyed_state::mem_window_state::MemoryWindowState;

pub mod mem_reducing_state;
pub mod mem_storage;
pub mod mem_window_state;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StateKey {
    pub(crate) window: Window,
    pub(crate) job_id: JobId,
    pub(crate) task_number: u16,
}

impl StateKey {
    pub fn new(window: Window, job_id: JobId, task_number: u16) -> Self {
        StateKey {
            window,
            job_id,
            task_number,
        }
    }
}

pub enum StateIterator {
    BTreeMap(Window, IntoIter<Record, Record>),
}

impl Iterator for StateIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            StateIterator::BTreeMap(window, iter) => iter.next().map(|(key, val)| {
                let mut key = key.clone();
                key.extend(val.clone()).expect("key value merge error");
                key.trigger_window = Some(window.clone());
                key
            }),
        }
    }
}

/// See flink `ReducingState`
pub trait TReducingState: Debug {
    fn get_mut(&mut self, key: &Record) -> Option<&mut Record>;
    fn insert(&mut self, key: Record, val: Record);
    fn flush(&mut self);
    fn snapshot(&mut self);
    fn close(self);
    fn destroy(self);
    fn iter(self) -> StateIterator;
    fn len(&self) -> usize;
}

#[derive(Debug)]
pub enum ReducingState {
    MemoryReducingState(MemoryReducingState),
}

impl ReducingState {
    pub fn new(state_key: &StateKey, mode: KeyedStateBackend) -> Option<ReducingState> {
        match mode {
            KeyedStateBackend::Memory => MemoryReducingState::from(state_key)
                .map(|state| ReducingState::MemoryReducingState(state)),
        }
    }
}

impl TReducingState for ReducingState {
    fn get_mut(&mut self, key: &Record) -> Option<&mut Record> {
        match self {
            ReducingState::MemoryReducingState(state) => state.get_mut(key),
        }
    }

    fn insert(&mut self, key: Record, val: Record) {
        match self {
            ReducingState::MemoryReducingState(state) => state.insert(key, val),
        }
    }

    fn flush(&mut self) {
        match self {
            ReducingState::MemoryReducingState(state) => state.flush(),
        }
    }

    fn snapshot(&mut self) {
        match self {
            ReducingState::MemoryReducingState(state) => state.snapshot(),
        }
    }

    fn close(self) {
        match self {
            ReducingState::MemoryReducingState(state) => state.close(),
        }
    }

    fn destroy(self) {
        match self {
            ReducingState::MemoryReducingState(state) => state.destroy(),
        }
    }

    fn iter(self) -> StateIterator {
        match self {
            ReducingState::MemoryReducingState(state) => state.iter(),
        }
    }

    fn len(&self) -> usize {
        match self {
            ReducingState::MemoryReducingState(state) => state.len(),
        }
    }
}

pub trait TWindowState: Debug {
    fn windows(&self) -> Vec<Window>;

    fn merge<F>(&mut self, key: Record, record: Record, reduce_fun: F)
    where
        F: Fn(Option<&mut Record>, &mut Record) -> Record;

    fn drop_window(&mut self, window: &Window);

    fn snapshot(&mut self, barrier: Barrier);
}

#[derive(Debug)]
pub enum WindowState {
    MemoryWindowState(MemoryWindowState),
}

impl WindowState {
    pub fn new(
        application_id: String,
        job_id: JobId,
        task_number: u16,
        mode: KeyedStateBackend,
    ) -> Self {
        match mode {
            KeyedStateBackend::Memory => WindowState::MemoryWindowState(MemoryWindowState::new(
                application_id,
                job_id,
                task_number,
            )),
        }
    }
}

impl TWindowState for WindowState {
    fn windows(&self) -> Vec<Window> {
        match self {
            WindowState::MemoryWindowState(state) => state.windows(),
        }
    }

    fn merge<F>(&mut self, key: Record, record: Record, reduce_fun: F)
    where
        F: Fn(Option<&mut Record>, &mut Record) -> Record,
    {
        match self {
            WindowState::MemoryWindowState(state) => state.merge(key, record, reduce_fun),
        }
    }

    fn drop_window(&mut self, window: &Window) {
        match self {
            WindowState::MemoryWindowState(state) => state.drop_window(window),
        }
    }

    fn snapshot(&mut self, barrier: Barrier) {
        match self {
            WindowState::MemoryWindowState(state) => state.snapshot(barrier),
        }
    }
}
