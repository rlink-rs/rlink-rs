use std::convert::TryFrom;
use std::sync::Mutex;

use crate::runtime::{ApplicationDescriptor, TaskManagerStatus};
use crate::storage::metadata::TMetadataStorage;
use crate::utils;

lazy_static! {
    pub static ref METADATA_STORAGE: Mutex<Option<ApplicationDescriptor>> = Mutex::new(None);
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MemoryMetadataStorage {}

impl MemoryMetadataStorage {
    pub fn new() -> Self {
        MemoryMetadataStorage {}
    }
}

impl TMetadataStorage for MemoryMetadataStorage {
    fn save(
        &mut self,
        metadata: ApplicationDescriptor,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut lock = METADATA_STORAGE
            .lock()
            .expect("METADATA_STORAGE lock failed");
        *lock = Some(metadata);

        debug!("Save metadata {:?}", lock.clone().unwrap());
        Ok(())
    }

    fn delete(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut lock = METADATA_STORAGE
            .lock()
            .expect("METADATA_STORAGE lock failed");
        *lock = None;

        debug!("Delete metadata {:?}", lock.clone().unwrap());
        Ok(())
    }

    fn load(&self) -> Result<ApplicationDescriptor, Box<dyn std::error::Error + Send + Sync>> {
        let lock = METADATA_STORAGE
            .lock()
            .expect("METADATA_STORAGE lock failed");
        Ok(lock.clone().unwrap())
    }

    fn update_job_status(
        &self,
        job_manager_status: TaskManagerStatus,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut lock = METADATA_STORAGE
            .lock()
            .expect("METADATA_STORAGE lock failed");
        let mut job_descriptor: ApplicationDescriptor = lock.clone().unwrap();
        job_descriptor.coordinator_manager.coordinator_status = job_manager_status;

        *lock = Some(job_descriptor);
        Ok(())
    }

    fn update_task_status(
        &self,
        task_manager_id: &str,
        task_manager_address: &str,
        task_manager_status: TaskManagerStatus,
        metrics_address: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut update_success = false;

        let mut lock = METADATA_STORAGE
            .lock()
            .expect("METADATA_STORAGE lock failed");
        let mut job_descriptor: ApplicationDescriptor = lock.clone().unwrap();
        for mut task_manager_descriptor in &mut job_descriptor.worker_managers {
            if task_manager_descriptor.task_manager_id.eq(task_manager_id) {
                task_manager_descriptor.task_manager_address = task_manager_address.to_string();
                task_manager_descriptor.task_status = task_manager_status;
                task_manager_descriptor.latest_heart_beat_ts =
                    utils::date_time::current_timestamp_millis();
                task_manager_descriptor.metrics_address = metrics_address.to_string();

                update_success = true;
                break;
            }
        }

        if update_success {
            debug!("Update TaskManager metadata success. {:?}", job_descriptor);
            *lock = Some(job_descriptor);
            Ok(())
        } else {
            error!(
                "TaskManager(task_manager_id={}) metadata not found",
                task_manager_id
            );
            Err(Box::try_from(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "metadata not found",
            ))
            .unwrap())
        }
    }
}
