use std::borrow::BorrowMut;
use std::convert::TryFrom;
use std::ops::Deref;
use std::time::Duration;

use crate::api::checkpoint::CheckpointHandle;
use crate::api::cluster::MetadataStorageType;
use crate::api::cluster::TaskResourceInfo;
use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::api::properties::{Properties, SYSTEM_CLUSTER_MODE};
use crate::dag::DagManager;
use crate::deployment::TResourceManager;
use crate::runtime::context::Context;
use crate::runtime::coordinator::checkpoint_manager::CheckpointManager;
use crate::runtime::coordinator::server::web_launch;
use crate::runtime::coordinator::task_distribution::build_job_descriptor;
use crate::runtime::{ApplicationDescriptor, TaskManagerStatus};
use crate::storage::metadata::{
    loop_delete_job_descriptor, loop_read_job_descriptor, loop_save_job_descriptor,
    loop_update_job_status, MetadataStorage,
};
use crate::utils::date_time::timestamp_str;

// pub mod checkpoint;
pub mod checkpoint_manager;
pub mod heart_beat;
pub mod server;
pub mod task_distribution;

pub(crate) struct CoordinatorTask<S, R>
where
    S: StreamApp + 'static,
    R: TResourceManager + 'static,
{
    context: Context,
    stream_app: S,
    metadata_storage_mode: MetadataStorageType,
    resource_manager: R,
    stream_env: StreamExecutionEnvironment,
}

impl<S, R> CoordinatorTask<S, R>
where
    S: StreamApp + 'static,
    R: TResourceManager + 'static,
{
    pub fn new(
        context: Context,
        stream_app: S,
        resource_manager: R,
        stream_env: StreamExecutionEnvironment,
    ) -> Self {
        let metadata_storage_mode = context.cluster_config.metadata_storage.clone();

        CoordinatorTask {
            context,
            stream_app,
            metadata_storage_mode,
            resource_manager,
            stream_env,
        }
    }

    pub fn run(&mut self) {
        info!("coordinator start with mode {}", self.context.manager_type);

        let application_properties = self.prepare_properties();

        self.stream_app
            .build_stream(&application_properties, self.stream_env.borrow_mut());

        let dag_manager = {
            let raw_stream_graph = self.stream_env.stream_manager.stream_graph.borrow();
            DagManager::try_from(raw_stream_graph.deref()).unwrap()
        };
        info!("StreamGraph: {}", dag_manager.stream_graph().to_string());
        info!("JobGraph: {}", dag_manager.job_graph().to_string());
        info!(
            "ExecutionGraph: {}",
            dag_manager.execution_graph().to_string()
        );

        let mut application_descriptor = self.build_metadata(&dag_manager, &application_properties);
        info!(
            "ApplicationDescriptor : {}",
            application_descriptor.to_string()
        );

        let ck_manager =
            self.build_checkpoint_manager(&dag_manager, application_descriptor.borrow_mut());
        info!("CheckpointManager create");

        self.web_serve(application_descriptor.borrow_mut(), ck_manager, dag_manager);
        info!(
            "serve coordinator web ui {}",
            &application_descriptor
                .coordinator_manager
                .coordinator_address
        );

        self.resource_manager
            .prepare(&self.context, &application_descriptor);
        info!("ResourceManager prepared");

        // loop restart all tasks when some task is failure
        loop {
            // save metadata to storage
            self.save_metadata(application_descriptor.clone());
            info!("save metadata to storage");

            // allocate all worker's resources
            let worker_task_ids = self.allocate_worker();
            info!("allocate workers success");

            // blocking util all worker's status is `Register` status
            self.waiting_worker_status_fine();
            info!("all worker status is fine");

            // heartbeat check. blocking util heartbeat timeout
            heart_beat::start_heart_beat_timer(self.metadata_storage_mode.clone());
            info!("heartbeat has interrupted");

            // heartbeat timeout and stop all worker's tasks
            self.stop_all_worker_tasks(worker_task_ids);
            info!("stop all workers");

            // clear metadata from storage
            self.clear_metadata();
            info!("clear metadata from storage");
        }
    }

    fn prepare_properties(&self) -> Properties {
        let mut job_properties = Properties::new();
        job_properties.set_str(
            SYSTEM_CLUSTER_MODE,
            format!("{}", self.context.cluster_mode).as_str(),
        );

        self.stream_app
            .prepare_properties(job_properties.borrow_mut());

        for (k, v) in job_properties.as_map() {
            info!("properties key={}, value={}", k, v);
        }

        job_properties
    }

    fn build_metadata(
        &mut self,
        dag_manager: &DagManager,
        job_properties: &Properties,
    ) -> ApplicationDescriptor {
        let application_descriptor = build_job_descriptor(
            self.stream_env.application_name.as_str(),
            dag_manager,
            job_properties,
            &self.context,
        );
        // let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        // loop_save_job_descriptor(metadata_storage.borrow_mut(), job_descriptor.clone());
        application_descriptor
    }

    fn save_metadata(&self, application_descriptor: ApplicationDescriptor) {
        let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        loop_save_job_descriptor(
            metadata_storage.borrow_mut(),
            application_descriptor.clone(),
        );
    }

    fn clear_metadata(&self) {
        let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        loop_delete_job_descriptor(metadata_storage.borrow_mut());
    }

    fn build_checkpoint_manager(
        &self,
        dag_manager: &DagManager,
        application_descriptor: &mut ApplicationDescriptor,
    ) -> CheckpointManager {
        let mut ck_manager =
            CheckpointManager::new(dag_manager, &self.context, application_descriptor);
        let operator_checkpoints = ck_manager.load().expect("load checkpoints error");
        if operator_checkpoints.len() == 0 {
            return ck_manager;
        }

        for task_manager_descriptor in &mut application_descriptor.worker_managers {
            for task_descriptor in &mut task_manager_descriptor.task_descriptors {
                for operator_id in &task_descriptor.operator_ids {
                    let cks = operator_checkpoints.get(&operator_id).unwrap();
                    if cks.len() == 0 {
                        info!("operator {:?} checkpoint not found", operator_id);
                        continue;
                    }

                    let ck = cks
                        .iter()
                        .find(|ck| ck.task_id.task_number == task_descriptor.task_id.task_number)
                        .unwrap();
                    task_descriptor.checkpoint_id = ck.checkpoint_id;
                    task_descriptor.checkpoint_handle = Some(CheckpointHandle {
                        handle: ck.handle.handle.clone(),
                    });
                    info!("task_descriptor {:?} checkpoint loaded", task_descriptor);
                }
            }
        }

        ck_manager
    }

    fn web_serve(
        &self,
        application_descriptor: &mut ApplicationDescriptor,
        checkpoint_manager: CheckpointManager,
        dag_manager: DagManager,
    ) {
        let context = self.context.clone();
        let metadata_storage_mode = self.metadata_storage_mode.clone();

        let address = web_launch(
            context,
            metadata_storage_mode,
            checkpoint_manager,
            dag_manager,
        );
        application_descriptor
            .coordinator_manager
            .coordinator_address = address;
    }

    fn allocate_worker(&self) -> Vec<TaskResourceInfo> {
        self.resource_manager
            .worker_allocate(&self.stream_app, &self.stream_env)
            .expect("try allocate worker error")
    }

    fn waiting_worker_status_fine(&self /*, metadata_storage: Box<dyn MetadataStorage>*/) {
        let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        loop {
            info!("waiting all workers status fine...");

            let job_descriptor = loop_read_job_descriptor(&metadata_storage);

            let unregister_worker = job_descriptor
                .worker_managers
                .iter()
                .find(|x| x.task_status.ne(&TaskManagerStatus::Registered));

            if unregister_worker.is_none() {
                loop_update_job_status(
                    metadata_storage.borrow_mut(),
                    TaskManagerStatus::Registered,
                );
                info!("all workers status fine and Job update state to `Registered`");
                job_descriptor.worker_managers.iter().for_each(|tm| {
                    info!(
                        "Registered List: `{}` registered at {}",
                        tm.task_manager_id,
                        timestamp_str(tm.latest_heart_beat_ts),
                    );
                });
                break;
            }

            std::thread::sleep(Duration::from_secs(3));
        }
    }

    fn stop_all_worker_tasks(&self, worker_task_ids: Vec<TaskResourceInfo>) {
        // loop stop all workers util all are success
        loop {
            let rt = self.resource_manager.stop_workers(worker_task_ids.clone());
            match rt {
                Ok(_) => {
                    info!("stop all workers");
                    break;
                }
                Err(e) => {
                    error!("try stop all workers error. {}", e);
                    std::thread::sleep(Duration::from_secs(2));
                }
            }
        }
    }
}
