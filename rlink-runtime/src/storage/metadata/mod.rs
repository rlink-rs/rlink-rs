pub mod mem_metadata_storage;
pub mod metadata_loader;

use rlink_core::cluster::{ManagerStatus, MetadataStorageType};
use rlink_core::{ClusterDescriptor, HeartbeatItem};

use crate::storage::metadata::mem_metadata_storage::MemoryMetadataStorage;

#[async_trait]
pub trait TMetadataStorage {
    /// save metadata to storage
    async fn save(&mut self, metadata: ClusterDescriptor) -> anyhow::Result<()>;

    /// load metadata from storage
    async fn load(&self) -> anyhow::Result<ClusterDescriptor>;

    /// update coordinator status change
    async fn update_coordinator_status(&self, status: ManagerStatus) -> anyhow::Result<()>;

    /// update worker status change, return the coordinator's status
    async fn update_worker_status(
        &self,
        task_manager_id: String,
        heartbeat_items: Vec<HeartbeatItem>,
        worker_manager_status: ManagerStatus,
    ) -> anyhow::Result<ManagerStatus>;
}

#[derive(Clone)]
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

#[async_trait]
impl TMetadataStorage for MetadataStorage {
    async fn save(&mut self, metadata: ClusterDescriptor) -> anyhow::Result<()> {
        match self {
            MetadataStorage::MemoryMetadataStorage(storage) => storage.save(metadata).await,
        }
    }

    async fn load(&self) -> anyhow::Result<ClusterDescriptor> {
        match self {
            MetadataStorage::MemoryMetadataStorage(storage) => storage.load().await,
        }
    }

    async fn update_coordinator_status(&self, status: ManagerStatus) -> anyhow::Result<()> {
        match self {
            MetadataStorage::MemoryMetadataStorage(storage) => {
                storage.update_coordinator_status(status).await
            }
        }
    }

    async fn update_worker_status(
        &self,
        task_manager_id: String,
        heartbeat_items: Vec<HeartbeatItem>,
        status: ManagerStatus,
    ) -> anyhow::Result<ManagerStatus> {
        match self {
            MetadataStorage::MemoryMetadataStorage(storage) => {
                storage
                    .update_worker_status(task_manager_id, heartbeat_items, status)
                    .await
            }
        }
    }
}

pub(crate) async fn loop_read_cluster_descriptor(
    metadata_storage: &MetadataStorage,
) -> ClusterDescriptor {
    loop_fn!(
        metadata_storage.load().await,
        std::time::Duration::from_secs(2)
    )
}

pub(crate) async fn loop_save_cluster_descriptor(
    metadata_storage: &mut MetadataStorage,
    cluster_descriptor: ClusterDescriptor,
) {
    loop_fn!(
        metadata_storage.save(cluster_descriptor.clone()).await,
        std::time::Duration::from_secs(2)
    );
}

pub(crate) async fn loop_update_application_status(
    metadata_storage: &mut MetadataStorage,
    coordinator_status: ManagerStatus,
) {
    loop_fn!(
        metadata_storage
            .update_coordinator_status(coordinator_status.clone())
            .await,
        std::time::Duration::from_secs(2)
    );
}
