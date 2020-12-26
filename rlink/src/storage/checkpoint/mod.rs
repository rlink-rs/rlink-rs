use crate::api::backend::CheckpointBackend;
use crate::api::checkpoint::Checkpoint;
use crate::runtime::ChainId;
use crate::storage::checkpoint::memory_checkpoint_storage::MemoryCheckpointStorage;
use crate::storage::checkpoint::mysql_checkpoint_storage::MySqlCheckpointStorage;

pub mod memory_checkpoint_storage;
pub mod mysql_checkpoint_storage;

pub trait CheckpointStorage {
    fn save(
        &mut self,
        job_name: &str,
        job_id: &str,
        chain_id: ChainId,
        checkpoint_id: u64,
        finish_cks: Vec<Checkpoint>,
        ttl: u64,
    ) -> anyhow::Result<()>;
    fn load(&mut self, job_name: &str, chain_id: ChainId) -> anyhow::Result<Vec<Checkpoint>>;
}

#[derive(Debug)]
pub enum CheckpointStorageWrap {
    MemoryCheckpointStorage(MemoryCheckpointStorage),
    MySqlCheckpointStorage(MySqlCheckpointStorage),
}

impl CheckpointStorageWrap {
    pub fn new(checkpoint_backend: &CheckpointBackend) -> Self {
        match checkpoint_backend {
            CheckpointBackend::Memory => {
                CheckpointStorageWrap::MemoryCheckpointStorage(MemoryCheckpointStorage::new())
            }
            CheckpointBackend::MySql { endpoint } => CheckpointStorageWrap::MySqlCheckpointStorage(
                MySqlCheckpointStorage::new(endpoint.as_str()),
            ),
        }
    }
}

impl CheckpointStorage for CheckpointStorageWrap {
    fn save(
        &mut self,
        job_name: &str,
        job_id: &str,
        chain_id: u32,
        checkpoint_id: u64,
        finish_cks: Vec<Checkpoint>,
        ttl: u64,
    ) -> anyhow::Result<()> {
        match self {
            CheckpointStorageWrap::MemoryCheckpointStorage(storage) => {
                storage.save(job_name, job_id, chain_id, checkpoint_id, finish_cks, ttl)
            }
            CheckpointStorageWrap::MySqlCheckpointStorage(storage) => {
                storage.save(job_name, job_id, chain_id, checkpoint_id, finish_cks, ttl)
            }
        }
    }

    fn load(&mut self, job_name: &str, chain_id: u32) -> anyhow::Result<Vec<Checkpoint>> {
        match self {
            CheckpointStorageWrap::MemoryCheckpointStorage(storage) => {
                storage.load(job_name, chain_id)
            }
            CheckpointStorageWrap::MySqlCheckpointStorage(storage) => {
                storage.load(job_name, chain_id)
            }
        }
    }
}
