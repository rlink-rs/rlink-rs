use crate::api::backend::KeyedStateBackend;
use crate::api::element::{Barrier, Record};
use crate::api::window::WindowWrap;
use crate::storage::keyed_state::mem_reducing_state::MemoryReducingState;
use crate::storage::keyed_state::mem_window_state::MemoryWindowState;
use std::collections::hash_map::Iter;
use std::fmt::Debug;

pub mod mem_reducing_state;
pub mod mem_storage;
pub mod mem_window_state;
// pub mod rocksdb_reducing_state;
// pub mod rocksdb_window_state;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StateKey {
    pub(crate) window: WindowWrap,
    pub(crate) chain_id: u32,
    pub(crate) task_number: u16,
}

impl StateKey {
    pub fn new(window: WindowWrap, chain_id: u32, task_number: u16) -> Self {
        StateKey {
            window,
            chain_id,
            task_number,
        }
    }

    // pub fn to_string(&self) -> String {
    //     format!(
    //         "{}-{}_{}-{}",
    //         // self.job_id,
    //         self.chain_id,
    //         utils::date_time::timestamp_to_path_string(self.window.min_timestamp()),
    //         utils::date_time::timestamp_to_path_string(self.window.max_timestamp()),
    //         self.task_number,
    //     )
    // }
}

pub enum StateIterator<'a> {
    HashMap(Iter<'a, Record, Record>),
    // RocksDB(DBIterator<'a>),
}

impl<'a> Iterator for StateIterator<'a> {
    type Item = (Record, Record);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            StateIterator::HashMap(iter) => {
                iter.next().map(|(key, val)| (key.clone(), val.clone()))
            }
            // StateIterator::RocksDB(iter) => iter.next().map(|kv| {
            //     let mut bytes_mut = BytesMut::from(&*(kv.0));
            //     let key = Record::deserialize(bytes_mut.borrow_mut());
            // 
            //     let mut bytes_mut1 = BytesMut::from(&*(kv.1));
            //     let val = Record::deserialize(bytes_mut1.borrow_mut());
            // 
            //     (key, val)
            // }),
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
    fn iter(&self) -> StateIterator;
    fn len(&self) -> usize;
}

#[derive(Debug)]
pub enum ReducingStateWrap {
    MemoryReducingState(MemoryReducingState),
    // RocksDBReducingState(RocksDBReducingState),
}

impl ReducingStateWrap {
    pub fn new(state_key: &StateKey, mode: KeyedStateBackend) -> Option<ReducingStateWrap> {
        match mode {
            KeyedStateBackend::Memory => MemoryReducingState::from(state_key)
                .map(|state| ReducingStateWrap::MemoryReducingState(state)),
            // KeyedStateBackend::RocksDBStateBackend(backend_path) => {
            //     Some(ReducingStateWrap::RocksDBReducingState(
            //         RocksDBReducingState::new(state_key, backend_path.as_str(), true),
            //     ))
            // }
        }
    }
}

impl ReducingState for ReducingStateWrap {
    fn get_mut(&mut self, key: &Record) -> Option<&mut Record> {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.get_mut(key),
            // ReducingStateWrap::RocksDBReducingState(state) => state.get(key),
        }
    }

    fn insert(&mut self, key: Record, val: Record) {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.insert(key, val),
            // ReducingStateWrap::RocksDBReducingState(state) => state.insert(key, val),
        }
    }

    fn flush(&mut self) {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.flush(),
            // ReducingStateWrap::RocksDBReducingState(state) => state.flush(),
        }
    }

    fn snapshot(&mut self) {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.snapshot(),
            // ReducingStateWrap::RocksDBReducingState(state) => state.snapshot(),
        }
    }

    fn close(self) {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.close(),
            // ReducingStateWrap::RocksDBReducingState(state) => state.close(),
        }
    }

    fn destroy(self) {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.destroy(),
            // ReducingStateWrap::RocksDBReducingState(state) => state.destroy(),
        }
    }

    fn iter(&self) -> StateIterator {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.iter(),
            // ReducingStateWrap::RocksDBReducingState(state) => state.iter(),
        }
    }

    fn len(&self) -> usize {
        match self {
            ReducingStateWrap::MemoryReducingState(state) => state.len(),
            // ReducingStateWrap::RocksDBReducingState(state) => state.capacity(),
        }
    }
}

// pub enum ReducingStateRef<'a, A, B> {
//     Hash(dashmap::mapref::one::Ref<'a, A, B>),
//     RocksDB(),
// }

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
    // RocksDBWindowState(RocksDBWindowState),
}

impl WindowStateWrap {
    pub fn new(job_id: String, chain_id: u32, task_number: u16, mode: KeyedStateBackend) -> Self {
        match mode {
            KeyedStateBackend::Memory => WindowStateWrap::MemoryWindowState(
                MemoryWindowState::new(job_id, chain_id, task_number),
            ),
            // KeyedStateBackend::RocksDBStateBackend(path) => WindowStateWrap::RocksDBWindowState(
            //     RocksDBWindowState::new(job_id, chain_id, task_number, path),
            // ),
        }
    }
}

impl WindowState for WindowStateWrap {
    fn windows(&self) -> Vec<WindowWrap> {
        match self {
            WindowStateWrap::MemoryWindowState(state) => state.windows(),
            // WindowStateWrap::RocksDBWindowState(state) => state.windows(),
        }
    }

    fn merge<F>(&mut self, key: Record, record: Record, reduce_fun: F)
    where
        F: Fn(Option<&mut Record>, &mut Record) -> Record,
    {
        match self {
            WindowStateWrap::MemoryWindowState(state) => state.merge(key, record, reduce_fun), // WindowStateWrap::RocksDBWindowState(state) => {
                                                                                               //     state.merge(windows, key, record, reduce_fun)
                                                                                               // }
        }
    }

    fn drop_window(&mut self, window: &WindowWrap) {
        match self {
            WindowStateWrap::MemoryWindowState(state) => state.drop_window(window),
            // WindowStateWrap::RocksDBWindowState(state) => state.drop_window(window),
        }
    }

    fn snapshot(&mut self, barrier: Barrier) {
        match self {
            WindowStateWrap::MemoryWindowState(state) => state.snapshot(barrier),
            // WindowStateWrap::RocksDBWindowState(state) => state.snapshot(barrier),
        }
    }
}
