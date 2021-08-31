use std::borrow::BorrowMut;
use std::sync::Mutex;

use crate::core::runtime::{ClusterDescriptor, ManagerStatus};
use crate::runtime::HeartbeatItem;
use crate::storage::metadata::TMetadataStorage;
use crate::utils::date_time::current_timestamp_millis;

lazy_static! {
    pub static ref METADATA_STORAGE: Mutex<Option<ClusterDescriptor>> = Mutex::new(None);
}

#[derive(Clone)]
pub struct MemoryMetadataStorage {}

impl MemoryMetadataStorage {
    pub fn new() -> Self {
        MemoryMetadataStorage {}
    }
}

impl TMetadataStorage for MemoryMetadataStorage {
    fn save(&mut self, metadata: ClusterDescriptor) -> anyhow::Result<()> {
        let mut lock = METADATA_STORAGE
            .lock()
            .expect("METADATA_STORAGE lock failed");
        *lock = Some(metadata);

        debug!("Save metadata {:?}", lock.as_ref());
        Ok(())
    }

    fn load(&self) -> anyhow::Result<ClusterDescriptor> {
        let lock = METADATA_STORAGE.lock().unwrap();
        lock.clone().ok_or(anyhow!("ClusterDescriptor not found"))
    }

    fn update_coordinator_status(&self, status: ManagerStatus) -> anyhow::Result<()> {
        let mut lock = METADATA_STORAGE.lock().unwrap();

        let cluster_descriptor = lock
            .as_mut()
            .ok_or(anyhow!("ClusterDescriptor not found"))?;
        cluster_descriptor.coordinator_manager.status = status;

        Ok(())
    }

    fn update_worker_status(
        &self,
        task_manager_id: String,
        heartbeat_items: Vec<HeartbeatItem>,
        status: ManagerStatus,
    ) -> anyhow::Result<ManagerStatus> {
        let mut lock = METADATA_STORAGE.lock().unwrap();
        let cluster_descriptor = (&mut *lock)
            .as_mut()
            .ok_or(anyhow!("ClusterDescriptor not found"))?;

        let task_manager_descriptor = cluster_descriptor
            .borrow_mut()
            .worker_managers
            .iter_mut()
            .find(|w| w.task_manager_id.eq(task_manager_id.as_str()))
            .ok_or(anyhow!(
                "TaskManager not found, task_manager_id={}",
                task_manager_id
            ))?;

        task_manager_descriptor.status = status;
        task_manager_descriptor.latest_heart_beat_ts = current_timestamp_millis();

        let mut exist_task_end_hb = false;
        for heartbeat_item in heartbeat_items {
            match heartbeat_item {
                HeartbeatItem::MetricsAddress(addr) => {
                    task_manager_descriptor.metrics_address = addr;
                }
                HeartbeatItem::WorkerManagerAddress(addr) => {
                    task_manager_descriptor.task_manager_address = addr;
                }
                HeartbeatItem::WorkerManagerWebAddress(addr) => {
                    task_manager_descriptor.web_address = addr;
                }
                HeartbeatItem::HeartBeatStatus(status) => {
                    task_manager_descriptor.latest_heart_beat_status = status;
                }
                HeartbeatItem::TaskThreadId { task_id, thread_id } => {
                    for task_descriptor in &mut task_manager_descriptor.task_descriptors {
                        if task_descriptor.task_id.eq(&task_id) {
                            task_descriptor.thread_id = format!("0x{:x}", thread_id);
                        }
                    }
                }
                HeartbeatItem::TaskEnd { task_id } => {
                    for task_descriptor in &mut task_manager_descriptor.task_descriptors {
                        if task_descriptor.task_id.eq(&task_id) {
                            task_descriptor.terminated = true;
                        }
                    }

                    exist_task_end_hb = true;
                    info!("Receiver `TaskEnd` heartbeat from {:?}", task_id);
                }
            }
        }

        if exist_task_end_hb {
            cluster_descriptor.flush_coordinator_status();
            info!(
                "Flush coordinator status: {:?}",
                cluster_descriptor.coordinator_manager.status
            );
        }

        debug!(
            "Update TaskManager metadata success. {:?}",
            cluster_descriptor
        );

        Ok(cluster_descriptor.coordinator_manager.status)
    }
}
