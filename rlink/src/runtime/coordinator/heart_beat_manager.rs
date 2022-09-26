use std::time::Duration;

use crate::core::cluster::MetadataStorageType;
use crate::core::runtime::ManagerStatus;
use crate::storage::metadata::{loop_read_cluster_descriptor, MetadataStorage};
use crate::utils;

pub enum HeartbeatResult {
    Timeout,
    End,
}

/// heartbeat timeout check
pub(crate) async fn start_heartbeat_timer(
    metadata_storage_mode: MetadataStorageType,
) -> HeartbeatResult {
    let metadata_storage = MetadataStorage::new(&metadata_storage_mode);
    loop {
        tokio::time::sleep(Duration::from_secs(3)).await;

        let cluster_descriptor = loop_read_cluster_descriptor(&metadata_storage).await;

        if cluster_descriptor.coordinator_manager.status == ManagerStatus::Terminated {
            return HeartbeatResult::End;
        }

        let current_timestamp = utils::date_time::current_timestamp().as_millis() as u64;
        for task_manager_descriptor in &cluster_descriptor.worker_managers {
            if current_timestamp < task_manager_descriptor.latest_heart_beat_ts {
                warn!(
                    "The worker({}) time is too fast, check ntpd server",
                    task_manager_descriptor.task_manager_address
                );
                continue;
            }

            let dur = Duration::from_millis(
                current_timestamp - task_manager_descriptor.latest_heart_beat_ts,
            );

            debug!(
                "heartbeat delay {}ms from TaskManager {}",
                dur.as_millis(),
                task_manager_descriptor.task_manager_address
            );

            if dur.as_secs() > 50 {
                error!(
                    "heartbeat's timestamp {} lag {}s from TaskManager {}, and break heartbeat",
                    task_manager_descriptor.latest_heart_beat_ts,
                    dur.as_secs(),
                    task_manager_descriptor.task_manager_address
                );
                return HeartbeatResult::Timeout;
            }
        }

        debug!(
            "all({}) task is final",
            cluster_descriptor.worker_managers.len()
        );
    }
}
