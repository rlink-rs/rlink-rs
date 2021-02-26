use std::ops::Deref;
use std::sync::RwLock;
use std::time::Duration;

use crate::api::cluster::MetadataStorageType;
use crate::runtime::{ClusterDescriptor, HeartBeatStatus};
use crate::storage::metadata::{loop_read_cluster_descriptor, MetadataStorage};
use crate::utils;

lazy_static! {
    pub(crate) static ref CLUSTER_DESCRIPTOR: RwLock<Option<ClusterDescriptor>> = RwLock::new(None);
}

pub enum HeartbeatResult {
    Timeout,
    End,
}

fn update_global_cluster_descriptor(cluster_descriptor: ClusterDescriptor) {
    let job_descriptor_rw: &RwLock<Option<ClusterDescriptor>> = &*CLUSTER_DESCRIPTOR;
    let mut j = job_descriptor_rw.write().unwrap();
    *j = Some(cluster_descriptor);
}

pub(crate) fn global_cluster_descriptor() -> Option<ClusterDescriptor> {
    let job_descriptor_rw: &RwLock<Option<ClusterDescriptor>> = &*CLUSTER_DESCRIPTOR;
    let j = job_descriptor_rw.read().unwrap();
    j.deref().clone()
}

pub(crate) fn start_heartbeat_timer(metadata_storage_mode: MetadataStorageType) -> HeartbeatResult {
    let metadata_storage = MetadataStorage::new(&metadata_storage_mode);
    loop {
        std::thread::sleep(Duration::from_secs(5));

        let job_descriptor = loop_read_cluster_descriptor(&metadata_storage);
        update_global_cluster_descriptor(job_descriptor.clone());

        let current_timestamp = utils::date_time::current_timestamp().as_millis() as u64;

        let mut end_task_managers = 0;
        for task_manager_descriptor in &job_descriptor.worker_managers {
            if let HeartBeatStatus::End = task_manager_descriptor.latest_heart_beat_status {
                end_task_managers += 1;
                continue;
            }
            if current_timestamp < task_manager_descriptor.latest_heart_beat_ts {
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
                    "heart beat lag {}s from TaskManager {}, and break heartbeat",
                    dur.as_secs(),
                    task_manager_descriptor.task_manager_address
                );
                return HeartbeatResult::Timeout;
            }
        }

        debug!(
            "all({}) task is final",
            job_descriptor.worker_managers.len()
        );

        if end_task_managers == job_descriptor.worker_managers.len() {
            return HeartbeatResult::End;
        }
    }
}
