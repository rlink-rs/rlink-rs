use crate::api::backend::CheckpointBackend;
use crate::api::checkpoint::Checkpoint;
use crate::api::runtime::{CheckpointId, JobId, OperatorId};
use crate::storage::checkpoint::memory_checkpoint_storage::MemoryCheckpointStorage;
use crate::storage::checkpoint::mysql_checkpoint_storage::MySqlCheckpointStorage;

pub mod memory_checkpoint_storage;
pub mod mysql_checkpoint_storage;

pub trait CheckpointStorage {
    fn save(
        &mut self,
        application_name: &str,
        application_id: &str,
        checkpoint_id: CheckpointId,
        finish_cks: Vec<Checkpoint>,
        ttl: u64,
    ) -> anyhow::Result<()>;
    fn load(
        &mut self,
        application_name: &str,
        job_id: JobId,
        operator_id: OperatorId,
    ) -> anyhow::Result<Vec<Checkpoint>>;
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
            CheckpointBackend::MySql { endpoint, table } => {
                CheckpointStorageWrap::MySqlCheckpointStorage(MySqlCheckpointStorage::new(
                    endpoint.clone(),
                    table.clone(),
                ))
            }
        }
    }
}

impl CheckpointStorage for CheckpointStorageWrap {
    fn save(
        &mut self,
        application_name: &str,
        application_id: &str,
        checkpoint_id: CheckpointId,
        finish_cks: Vec<Checkpoint>,
        ttl: u64,
    ) -> anyhow::Result<()> {
        match self {
            CheckpointStorageWrap::MemoryCheckpointStorage(storage) => storage.save(
                application_name,
                application_id,
                checkpoint_id,
                finish_cks,
                ttl,
            ),
            CheckpointStorageWrap::MySqlCheckpointStorage(storage) => storage.save(
                application_name,
                application_id,
                checkpoint_id,
                finish_cks,
                ttl,
            ),
        }
    }

    fn load(
        &mut self,
        application_name: &str,
        job_id: JobId,
        operator_id: OperatorId,
    ) -> anyhow::Result<Vec<Checkpoint>> {
        match self {
            CheckpointStorageWrap::MemoryCheckpointStorage(storage) => {
                storage.load(application_name, job_id, operator_id)
            }
            CheckpointStorageWrap::MySqlCheckpointStorage(storage) => {
                storage.load(application_name, job_id, operator_id)
            }
        }
    }
}
