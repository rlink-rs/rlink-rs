use std::fmt::Debug;

use crate::api::cluster::MetadataStorageType;
use crate::runtime::{ApplicationDescriptor, TaskManagerStatus};
use crate::storage::metadata::mem_metadata_storage::MemoryMetadataStorage;

pub mod mem_metadata_storage;

pub mod metadata_loader;
pub(crate) use metadata_loader::MetadataLoader;

pub trait TMetadataStorage: Debug {
    fn save(&mut self, metadata: ApplicationDescriptor) -> anyhow::Result<()>;
    fn delete(&mut self) -> anyhow::Result<()>;
    fn load(&self) -> anyhow::Result<ApplicationDescriptor>;
    fn update_application_status(
        &self,
        job_manager_status: TaskManagerStatus,
    ) -> anyhow::Result<()>;
    fn update_task_manager_status(
        &self,
        task_manager_id: &str,
        task_manager_address: &str,
        task_manager_status: TaskManagerStatus,
        metrics_address: &str,
    ) -> anyhow::Result<()>;
}

#[derive(Debug)]
pub enum MetadataStorage {
    MemoryMetadataStorage(MemoryMetadataStorage),
}

impl MetadataStorage {
    pub fn new(mode: &MetadataStorageType) -> Self {
        match mode {
            MetadataStorageType::Memory => {
                let storage = MemoryMetadataStorage::new();
                MetadataStorage::MemoryMetadataStorage(storage)
            }
        }
    }
}

impl TMetadataStorage for MetadataStorage {
    fn save(&mut self, metadata: ApplicationDescriptor) -> anyhow::Result<()> {
        match self {
            MetadataStorage::MemoryMetadataStorage(storage) => storage.save(metadata),
        }
    }

    fn delete(&mut self) -> anyhow::Result<()> {
        match self {
            MetadataStorage::MemoryMetadataStorage(storage) => storage.delete(),
        }
    }

    fn load(&self) -> anyhow::Result<ApplicationDescriptor> {
        match self {
            MetadataStorage::MemoryMetadataStorage(storage) => storage.load(),
        }
    }

    fn update_application_status(
        &self,
        job_manager_status: TaskManagerStatus,
    ) -> anyhow::Result<()> {
        match self {
            MetadataStorage::MemoryMetadataStorage(storage) => {
                storage.update_application_status(job_manager_status)
            }
        }
    }

    fn update_task_manager_status(
        &self,
        task_manager_id: &str,
        task_manager_address: &str,
        task_manager_status: TaskManagerStatus,
        metrics_address: &str,
    ) -> anyhow::Result<()> {
        match self {
            MetadataStorage::MemoryMetadataStorage(storage) => storage.update_task_manager_status(
                task_manager_id,
                task_manager_address,
                task_manager_status,
                metrics_address,
            ),
        }
    }
}

pub(crate) fn loop_read_job_descriptor(
    metadata_storage: &MetadataStorage,
) -> ApplicationDescriptor {
    loop_fn!(metadata_storage.load(), std::time::Duration::from_secs(2))
}

pub(crate) fn loop_save_job_descriptor(
    metadata_storage: &mut MetadataStorage,
    application_descriptor: ApplicationDescriptor,
) {
    loop_fn!(
        metadata_storage.save(application_descriptor.clone()),
        std::time::Duration::from_secs(2)
    );
}

pub(crate) fn loop_delete_job_descriptor(metadata_storage: &mut MetadataStorage) {
    loop_fn!(metadata_storage.delete(), std::time::Duration::from_secs(2));
}

pub(crate) fn loop_update_application_status(
    metadata_storage: &mut MetadataStorage,
    job_manager_status: TaskManagerStatus,
) {
    loop_fn!(
        metadata_storage.update_application_status(job_manager_status.clone()),
        std::time::Duration::from_secs(2)
    );
}
