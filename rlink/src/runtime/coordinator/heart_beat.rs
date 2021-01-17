use std::ops::Deref;
use std::sync::RwLock;
use std::time::Duration;

use crate::api::cluster::MetadataStorageMode;
use crate::runtime::ApplicationDescriptor;
use crate::storage::metadata::{loop_read_job_descriptor, MetadataStorageWrap};
use crate::utils;

lazy_static! {
    pub(crate) static ref JOB_DESCRIPTOR: RwLock<Option<ApplicationDescriptor>> = RwLock::new(None);
}

fn update_global_job_descriptor(job_descriptor: ApplicationDescriptor) {
    let job_descriptor_rw: &RwLock<Option<ApplicationDescriptor>> = &*JOB_DESCRIPTOR;
    let mut j = job_descriptor_rw.write().unwrap();
    *j = Some(job_descriptor);
}

pub(crate) fn get_global_job_descriptor() -> Option<ApplicationDescriptor> {
    let job_descriptor_rw: &RwLock<Option<ApplicationDescriptor>> = &*JOB_DESCRIPTOR;
    let j = job_descriptor_rw.read().unwrap();
    j.deref().clone()
}

pub(crate) fn start_heart_beat_timer(metadata_storage_mode: MetadataStorageMode) {
    let metadata_storage = MetadataStorageWrap::new(&metadata_storage_mode);
    loop {
        std::thread::sleep(Duration::from_secs(5));

        let job_descriptor = loop_read_job_descriptor(&metadata_storage);
        update_global_job_descriptor(job_descriptor.clone());

        let current_timestamp = utils::date_time::current_timestamp().as_millis() as u64;

        for task_manager_descriptor in &job_descriptor.worker_managers {
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
                return;
            }
        }

        debug!(
            "all({}) task is final",
            job_descriptor.worker_managers.len()
        );
    }
}
