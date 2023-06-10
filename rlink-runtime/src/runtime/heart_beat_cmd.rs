use std::time::Duration;

use anyhow::Ok;
use rlink_core::cluster::{ManagerStatus, MetadataStorageType};
use rlink_core::cmd::{CmdClient, CmdFactory, CmdServer};
use rlink_core::HeartbeatRequest;

use crate::storage::metadata::{MetadataStorage, TMetadataStorage};

pub struct HeartbeatServer {
    metadata_storage_mode: MetadataStorageType,
}

impl HeartbeatServer {
    pub fn new(metadata_storage_mode: MetadataStorageType) -> Self {
        Self {
            metadata_storage_mode,
        }
    }
}

#[async_trait]
impl CmdServer for HeartbeatServer {
    async fn request(&self, data: &[u8]) -> anyhow::Result<String> {
        let HeartbeatRequest {
            task_manager_id,
            change_items,
        } = serde_json::from_slice(data)?;

        debug!(
            "<heartbeat> from {}, items: {:?}",
            task_manager_id, change_items
        );

        let metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        let coordinator_status = metadata_storage
            .update_worker_status(task_manager_id, change_items, ManagerStatus::Registered)
            .await?;

        serde_json::to_string(&coordinator_status).map_err(|e| anyhow!(e))
    }
}

pub struct HeartbeatClient {}

#[async_trait]
impl CmdClient for HeartbeatClient {
    async fn submit(&self, _data: &[u8]) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct HeartbeatCmdFactory {
    metadata_storage_mode: MetadataStorageType,
}

impl CmdFactory for HeartbeatCmdFactory {
    fn name(&self) -> &str {
        "system.heartbeat"
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(10)
    }

    fn create_server(&self) -> Box<dyn CmdServer> {
        Box::new(HeartbeatServer {
            metadata_storage_mode: self.metadata_storage_mode.clone(),
        })
    }

    fn create_client(&self) -> Box<dyn CmdClient> {
        Box::new(HeartbeatClient {})
    }
}
