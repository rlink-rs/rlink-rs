use std::borrow::BorrowMut;
use std::time::Duration;

use crate::api::checkpoint::CheckpointHandle;
use crate::api::cluster::TaskResourceInfo;
use crate::api::env::{StreamExecutionEnvironment, StreamJob};
use crate::api::metadata::MetadataStorageMode;
use crate::api::properties::{Properties, SYSTEM_CLUSTER_MODE};
use crate::deployment::ResourceManager;
use crate::graph::{build_logic_plan, JobGraph};
use crate::runtime::context::Context;
use crate::runtime::coordinator::checkpoint_manager::CheckpointManager;
use crate::runtime::coordinator::server::web_launch;
use crate::runtime::coordinator::task_distribution::build_job_descriptor;
use crate::runtime::{JobDescriptor, TaskManagerStatus};
use crate::storage::metadata::MetadataLoader;
use crate::storage::metadata::{
    loop_delete_job_descriptor, loop_read_job_descriptor, loop_save_job_descriptor,
    loop_update_job_status, MetadataStorageWrap,
};
use crate::utils::date_time::timestamp_str;

// pub mod checkpoint;
pub mod checkpoint_manager;
pub mod heart_beat;
pub mod server;
pub mod task_distribution;

pub(crate) struct CoordinatorTask<S, R>
where
    S: StreamJob + 'static,
    R: ResourceManager + 'static,
{
    context: Context,
    stream_job: S,
    metadata_storage_mode: MetadataStorageMode,
    resource_manager: R,
    stream_env: StreamExecutionEnvironment,
}

impl<S, R> CoordinatorTask<S, R>
where
    S: StreamJob + 'static,
    R: ResourceManager + 'static,
{
    pub fn new(
        context: Context,
        stream_job: S,
        resource_manager: R,
        stream_env: StreamExecutionEnvironment,
    ) -> Self {
        let metadata_storage_mode = {
            let job_id = context.job_id.as_str();
            let mode = context.cluster_config.metadata_storage_mode.as_str();
            let endpoints = &context.cluster_config.metadata_storage_endpoints;
            MetadataStorageMode::from(mode, endpoints, job_id)
        };

        CoordinatorTask {
            context,
            stream_job,
            metadata_storage_mode,
            resource_manager,
            stream_env,
        }
    }

    pub fn run(&mut self) {
        info!("coordinator start with mode {}", self.context.manager_type);

        let job_properties = self.prepare_properties();

        let data_stream = self
            .stream_job
            .build_stream(&job_properties, &self.stream_env);
        info!("DataStream: {:?}", data_stream);

        let logic_plan = build_logic_plan(data_stream, MetadataLoader::place_holder());
        info!(
            "Logic Plan: {}",
            serde_json::to_string_pretty(&logic_plan).unwrap()
        );

        let mut job_descriptor = self.build_metadata(&logic_plan, &job_properties);
        info!("JobDescriptor : {:?}", &job_descriptor);

        let ck_manager = self.build_checkpoint_manager(&logic_plan, job_descriptor.borrow_mut());
        info!("CheckpointManager create");

        self.web_serve(job_descriptor.borrow_mut(), ck_manager);
        info!(
            "serve coordinator web ui {}",
            &job_descriptor.job_manager.coordinator_address
        );

        self.resource_manager
            .prepare(&self.context, &job_descriptor);
        info!("ResourceManager prepared");

        // loop restart all tasks when some task is failure
        loop {
            // save metadata to storage
            self.save_metadata(job_descriptor.clone());
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

        self.stream_job
            .prepare_properties(job_properties.borrow_mut());

        for (k, v) in job_properties.as_map() {
            info!("properties key={}, value={}", k, v);
        }

        job_properties
    }

    fn build_metadata(
        &mut self,
        logic_plan: &JobGraph,
        job_properties: &Properties,
    ) -> JobDescriptor {
        let job_descriptor = build_job_descriptor(
            self.stream_env.job_name.as_str(),
            logic_plan,
            job_properties,
            &self.context,
        );
        // let mut metadata_storage = MetadataStorageWrap::new(&self.metadata_storage_mode);
        // loop_save_job_descriptor(metadata_storage.borrow_mut(), job_descriptor.clone());
        job_descriptor
    }

    fn save_metadata(&self, job_descriptor: JobDescriptor) {
        let mut metadata_storage = MetadataStorageWrap::new(&self.metadata_storage_mode);
        loop_save_job_descriptor(metadata_storage.borrow_mut(), job_descriptor.clone());
    }

    fn clear_metadata(&self) {
        let mut metadata_storage = MetadataStorageWrap::new(&self.metadata_storage_mode);
        loop_delete_job_descriptor(metadata_storage.borrow_mut());
    }

    fn build_checkpoint_manager(
        &self,
        job_graph: &JobGraph,
        job_descriptor: &mut JobDescriptor,
    ) -> CheckpointManager {
        let mut ck_manager = CheckpointManager::new(job_graph, &self.context, &job_descriptor);
        let chain_checkpoints = ck_manager.load().expect("load checkpoints error");
        if chain_checkpoints.len() == 0 {
            return ck_manager;
        }

        for task_manager_descriptor in &mut job_descriptor.task_managers {
            for (chain_id, tasks_desc) in &mut task_manager_descriptor.chain_tasks {
                let cks = chain_checkpoints.get(chain_id).unwrap();
                if cks.len() == 0 {
                    info!("chain_id {} checkpoint not found", chain_id);
                    continue;
                }

                // todo case compatible `ck.task_num != task_desc.task_number`
                tasks_desc.iter_mut().for_each(|task_desc| {
                    let ck = cks
                        .iter()
                        .find(|ck| ck.task_num == task_desc.task_number)
                        .unwrap();
                    task_desc.checkpoint_id = ck.checkpoint_id;
                    task_desc.checkpoint_handle = Some(CheckpointHandle {
                        handle: ck.handle.handle.clone(),
                    });
                });
                info!("chain_id {} checkpoint loaded", chain_id);
            }
        }

        ck_manager
    }

    fn web_serve(&self, job_descriptor: &mut JobDescriptor, checkpoint_manager: CheckpointManager) {
        let context = self.context.clone();
        let metadata_storage_mode = self.metadata_storage_mode.clone();

        let address = web_launch(context, metadata_storage_mode, checkpoint_manager);
        job_descriptor.job_manager.coordinator_address = address;
    }

    fn allocate_worker(&self) -> Vec<TaskResourceInfo> {
        self.resource_manager
            .worker_allocate(&self.stream_job, &self.stream_env)
            .expect("try allocate worker error")
    }

    fn waiting_worker_status_fine(&self /*, metadata_storage: Box<dyn MetadataStorage>*/) {
        let mut metadata_storage = MetadataStorageWrap::new(&self.metadata_storage_mode);
        loop {
            info!("waiting all workers status fine...");

            let job_descriptor = loop_read_job_descriptor(&metadata_storage);

            let unregister_worker = job_descriptor
                .task_managers
                .iter()
                .find(|x| x.task_status.ne(&TaskManagerStatus::Registered));

            if unregister_worker.is_none() {
                loop_update_job_status(
                    metadata_storage.borrow_mut(),
                    TaskManagerStatus::Registered,
                );
                info!("all workers status fine and Job update state to `Registered`");
                job_descriptor.task_managers.iter().for_each(|tm| {
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
