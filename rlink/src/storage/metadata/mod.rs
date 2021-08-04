use std::fmt::Debug;

use crate::core::cluster::MetadataStorageType;
use crate::runtime::{ClusterDescriptor, HeartbeatItem, TaskManagerStatus};
use crate::storage::metadata::mem_metadata_storage::MemoryMetadataStorage;

pub mod mem_metadata_storage;

pub mod metadata_loader;
pub(crate) use metadata_loader::MetadataLoader;

pub trait TMetadataStorage: Debug {
    fn save(&mut self, metadata: ClusterDescriptor) -> anyhow::Result<()>;
    fn delete(&mut self) -> anyhow::Result<()>;
    fn load(&self) -> anyhow::Result<ClusterDescriptor>;
    fn update_application_status(
        &self,
        job_manager_status: TaskManagerStatus,
    ) -> anyhow::Result<()>;
    fn update_task_manager_status(
        &self,
        task_manager_id: String,
        heartbeat_items: Vec<HeartbeatItem>,
        task_manager_status: TaskManagerStatus,
    ) -> anyhow::Result<TaskManagerStatus>;
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
    fn save(&mut self, metadata: ClusterDescriptor) -> anyhow::Result<()> {
        match self {
            MetadataStorage::MemoryMetadataStorage(storage) => storage.save(metadata),
        }
    }

    fn delete(&mut self) -> anyhow::Result<()> {
        match self {
            MetadataStorage::MemoryMetadataStorage(storage) => storage.delete(),
        }
    }

    fn load(&self) -> anyhow::Result<ClusterDescriptor> {
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
        task_manager_id: String,
        heartbeat_items: Vec<HeartbeatItem>,
        task_manager_status: TaskManagerStatus,
    ) -> anyhow::Result<TaskManagerStatus> {
        match self {
            MetadataStorage::MemoryMetadataStorage(storage) => storage.update_task_manager_status(
                task_manager_id,
                heartbeat_items,
                task_manager_status,
            ),
        }
    }
}

pub(crate) fn loop_read_cluster_descriptor(
    metadata_storage: &MetadataStorage,
) -> ClusterDescriptor {
    loop_fn!(metadata_storage.load(), std::time::Duration::from_secs(2))
}

pub(crate) fn loop_save_cluster_descriptor(
    metadata_storage: &mut MetadataStorage,
    cluster_descriptor: ClusterDescriptor,
) {
    loop_fn!(
        metadata_storage.save(cluster_descriptor.clone()),
        std::time::Duration::from_secs(2)
    );
}

pub(crate) fn loop_delete_cluster_descriptor(metadata_storage: &mut MetadataStorage) {
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
