use std::collections::hash_map::IntoIter;
use std::fmt::Debug;

use crate::api::backend::KeyedStateBackend;
use crate::api::element::{Barrier, Record};
use crate::api::window::WindowWrap;
use crate::storage::keyed_state::mem_reducing_state::MemoryReducingState;
use crate::storage::keyed_state::mem_window_state::MemoryWindowState;

pub mod mem_reducing_state;
pub mod mem_storage;
pub mod mem_window_state;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StateKey {
    pub(crate) window: WindowWrap,
    pub(crate) job_id: u32,
    pub(crate) task_number: u16,
}

impl StateKey {
    pub fn new(window: WindowWrap, job_id: u32, task_number: u16) -> Self {
        StateKey {
            window,
            job_id,
            task_number,
        }
    }
}

pub enum StateIterator {
    HashMap(WindowWrap, IntoIter<Record, Record>),
}

impl Iterator for StateIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            StateIterator::HashMap(window, iter) => iter.next().map(|(key, val)| {
                let mut key = key.clone();
                key.extend(val.clone()).expect("key value merge error");
                key.trigger_window = Some(window.clone());
                key
            }),
        }
    }
}

/// See flink `ReducingState`
pub trait ReducingState: Debug {
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
pub enum ReducingStateWrap {
    MemoryReducingState(MemoryReducingState),
}

impl ReducingStateWrap {
    pub fn new(state_key: &StateKey, mode: KeyedStateBackend) -> Option<ReducingStateWrap> {
        match mode {
            KeyedStateBackend::Memory => MemoryReducingState::from(state_key)
                .map(|state| ReducingStateWrap::MemoryReducingState(state)),
        }
    }
}

impl ReducingState for ReducingStateWrap {
    fn get_mut(&mut self, key: &Record) -> Option<&mut Record> {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.get_mut(key),
        }
    }

    fn insert(&mut self, key: Record, val: Record) {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.insert(key, val),
        }
    }

    fn flush(&mut self) {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.flush(),
        }
    }

    fn snapshot(&mut self) {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.snapshot(),
        }
    }

    fn close(self) {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.close(),
        }
    }

    fn destroy(self) {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.destroy(),
        }
    }

    fn iter(self) -> StateIterator {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.iter(),
        }
    }

    fn len(&self) -> usize {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.len(),
        }
    }
}

pub trait WindowState: Debug {
    fn windows(&self) -> Vec<WindowWrap>;

    fn merge<F>(&mut self, key: Record, record: Record, reduce_fun: F)
    where
        F: Fn(Option<&mut Record>, &mut Record) -> Record;

    fn drop_window(&mut self, window: &WindowWrap);

    fn snapshot(&mut self, barrier: Barrier);
}

#[derive(Debug)]
pub enum WindowStateWrap {
    MemoryWindowState(MemoryWindowState),
}

impl WindowStateWrap {
    pub fn new(
        application_id: String,
        job_id: u32,
        task_number: u16,
        mode: KeyedStateBackend,
    ) -> Self {
        match mode {
            KeyedStateBackend::Memory => WindowStateWrap::MemoryWindowState(
                MemoryWindowState::new(application_id, job_id, task_number),
            ),
        }
    }
}

impl WindowState for WindowStateWrap {
    fn windows(&self) -> Vec<WindowWrap> {
        match self {
            WindowStateWrap::MemoryWindowState(state) => state.windows(),
        }
    }

    fn merge<F>(&mut self, key: Record, record: Record, reduce_fun: F)
    where
        F: Fn(Option<&mut Record>, &mut Record) -> Record,
    {
        match self {
            WindowStateWrap::MemoryWindowState(state) => state.merge(key, record, reduce_fun),
        }
    }

    fn drop_window(&mut self, window: &WindowWrap) {
        match self {
            WindowStateWrap::MemoryWindowState(state) => state.drop_window(window),
        }
    }

    fn snapshot(&mut self, barrier: Barrier) {
        match self {
            WindowStateWrap::MemoryWindowState(state) => state.snapshot(barrier),
        }
    }
}
