use crate::api::element::{Element, Record, Serde};
use crate::storage::keyed_state::{ReducingState, StateIterator, StateKey};
use crate::utils;
use bytes::BytesMut;
// use metrics::counter;
use rocksdb::checkpoint::Checkpoint;
use rocksdb::{BlockBasedOptions, IteratorMode, Options, WriteBatch, WriteOptions, DB};
use std::borrow::BorrowMut;
use std::iter::FromIterator;
use std::path::PathBuf;

#[inline]
fn get_db_path(state_key: &StateKey) -> PathBuf {
    utils::get_work_space().join(format!("state/{}.db", state_key.to_string()))
}

#[inline]
fn get_checkpoint_path(state_key: &StateKey) -> PathBuf {
    utils::get_work_space().join(format!("state/{}.ckp", state_key.to_string()))
}

#[derive(Debug)]
pub struct RocksDBReducingState {
    state_key: StateKey,
    backend_path: String,

    db: DB,
}

impl RocksDBReducingState {
    pub fn new(state_key: &StateKey, backend_path: &str, read_only: bool) -> Self {
        let path = get_db_path(state_key);

        info!("Try to open RocksDB [readonly={}] in {:?}", read_only, path);

        let mut db_opts = Options::default();
        let db = if read_only {
            DB::open_for_read_only(&db_opts, path.clone(), true).unwrap_or_else(|e| {
                panic!("open RocksDB({:?}) error.{}", path, e);
            })
        } else {
            let mut block_opts = BlockBasedOptions::default();
            block_opts.set_block_size(128 * 1024); // 128K
            block_opts.set_lru_cache(4 * 1024 * 1024); // 16M

            db_opts.create_missing_column_families(true);
            db_opts.create_if_missing(true);
            db_opts.set_use_fsync(false);
            db_opts.set_write_buffer_size(16 * 1024 * 1024);
            db_opts.set_max_background_compactions(1);
            db_opts.set_max_background_flushes(1);
            db_opts.set_block_based_table_factory(&block_opts);
            DB::open(&db_opts, path.clone()).unwrap_or_else(|e| {
                panic!("open RocksDB({:?}) error.{}", path, e);
            })
        };

        RocksDBReducingState {
            state_key: state_key.clone(),
            backend_path: backend_path.to_string(),
            db,
        }
    }
}

impl ReducingState for RocksDBReducingState {
    fn get(&self, key: &Record) -> Option<Record> {
        match self.db.get(key.to_bytes().to_vec()) {
            Ok(value) => value.map(|v| {
                let mut bytes_mut = BytesMut::from_iter(v);
                Element::deserialize(bytes_mut.borrow_mut()).into_record()
            }),
            Err(e) => {
                error!("RocksDB `get` error. {}", e);
                None
            }
        }
    }

    fn insert(&mut self, key: Record, val: Record) {
        let mut batch = WriteBatch::default();
        batch.put(key.to_bytes().to_vec(), val.to_bytes().to_vec());

        let mut write_opts = WriteOptions::default();
        write_opts.disable_wal(true);

        match self.db.write_opt(batch, &write_opts) {
            Ok(_) => {}
            Err(e) => error!("RocksDB `write` error. {}", e),
        }

        // counter!(format!("RocksDB.State.{}", self.state_key.to_string()), 1);
    }

    fn flush(&mut self) {
        match self.db.flush() {
            Ok(_) => {}
            Err(e) => error!("RocksDB flush error. {}", e),
        }
    }

    fn snapshot(&mut self) {
        match Checkpoint::new(&self.db) {
            Ok(checkpoint) => {
                let path = get_checkpoint_path(&self.state_key);
                match checkpoint.create_checkpoint(path) {
                    Ok(_) => info!(
                        "RocksDB create checkpoint({}) success.",
                        self.state_key.to_string()
                    ),
                    Err(e) => error!(
                        "RocksDB create checkpoint({}) error. {}",
                        self.state_key.to_string(),
                        e
                    ),
                }
            }
            Err(e) => {
                error!(
                    "RocksDB new checkpoint({}) error. {}",
                    self.state_key.to_string(),
                    e
                );
            }
        }
    }

    fn close(self) {
        info!("close RocksDB {:?}", get_db_path(&self.state_key));
        drop(self)
    }

    fn destroy(self) {
        let path = get_db_path(&self.state_key);
        info!("destroy RocksDB {:?}", path);

        self.close();

        let opts = Options::default();
        DB::destroy(&opts, path).expect("destroy RocksDB error");
    }

    fn iter(&self) -> StateIterator {
        let db_iterator = self.db.iterator(IteratorMode::Start);

        StateIterator::RocksDB(db_iterator)
    }
}

// #[cfg(test)]
// mod tests {
//     use crate::api::window::{TimeWindow, WindowWrap};
//     use crate::storage::keyed_state::rocksdb_reducing_state::RocksDBReducingState;
//     use crate::storage::keyed_state::{ReducingState, StateKey};
//
//     #[test]
//     pub fn record_test() {
//         for i in 0..100 {
//             let key_state = StateKey::new(
//                 "job_id".to_string(),
//                 WindowWrap::TimeWindow(TimeWindow::new(i, i + 1)),
//                 1,
//                 0,
//             );
//             {
//                 let window_state = RocksDBReducingState::new(&key_state, "", false);
//                 window_state.close();
//             }
//
//             {
//                 let window_state = RocksDBReducingState::new(&key_state, "", true);
//                 window_state.close();
//             }
//         }
//     }
// }
