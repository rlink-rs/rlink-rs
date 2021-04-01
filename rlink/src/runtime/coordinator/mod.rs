use std::borrow::BorrowMut;
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::api::checkpoint::CheckpointHandle;
use crate::api::cluster::MetadataStorageType;
use crate::api::cluster::TaskResourceInfo;
use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::api::properties::{Properties, SYSTEM_CLUSTER_MODE};
use crate::dag::metadata::DagMetadata;
use crate::dag::DagManager;
use crate::deployment::TResourceManager;
use crate::metrics::register_gauge;
use crate::runtime::context::Context;
use crate::runtime::coordinator::checkpoint_manager::CheckpointManager;
use crate::runtime::coordinator::heart_beat_manager::HeartbeatResult;
use crate::runtime::coordinator::task_distribution::build_cluster_descriptor;
use crate::runtime::coordinator::web_server::web_launch;
use crate::runtime::{ClusterDescriptor, TaskManagerStatus};
use crate::storage::metadata::{
    loop_delete_cluster_descriptor, loop_read_cluster_descriptor, loop_save_cluster_descriptor,
    loop_update_application_status, MetadataStorage,
};
use crate::utils::date_time::timestamp_str;

pub mod checkpoint_manager;
pub mod heart_beat_manager;
pub mod task_distribution;
pub mod web_server;

pub(crate) struct CoordinatorTask<S, R>
where
    S: StreamApp + 'static,
    R: TResourceManager + 'static,
{
    context: Arc<Context>,
    stream_app: S,
    metadata_storage_mode: MetadataStorageType,
    resource_manager: R,
    stream_env: StreamExecutionEnvironment,

    startup_number: Arc<AtomicI64>,
}

impl<S, R> CoordinatorTask<S, R>
where
    S: StreamApp + 'static,
    R: TResourceManager + 'static,
{
    pub fn new(
        context: Arc<Context>,
        stream_app: S,
        resource_manager: R,
        stream_env: StreamExecutionEnvironment,
    ) -> Self {
        let metadata_storage_mode = context.cluster_config.metadata_storage.clone();

        let startup_number = Arc::new(AtomicI64::default());
        register_gauge("startup_number", vec![], startup_number.clone());

        CoordinatorTask {
            context,
            stream_app,
            metadata_storage_mode,
            resource_manager,
            stream_env,
            startup_number,
        }
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        info!("coordinator start with mode {}", self.context.manager_type);

        let application_properties = self.prepare_properties();

        self.stream_app
            .build_stream(&application_properties, self.stream_env.borrow_mut());

        let dag_manager = {
            let raw_stream_graph = self.stream_env.stream_manager.stream_graph.borrow();
            DagManager::try_from(raw_stream_graph.deref())?
        };
        info!("DagManager build success");

        let dag_metadata = DagMetadata::from(&dag_manager);
        debug!("DagMetadata: {}", dag_metadata.to_string());

        let mut cluster_descriptor = self.build_metadata(&dag_manager, &application_properties);
        debug!("ApplicationDescriptor : {}", cluster_descriptor.to_string());

        let ck_manager =
            self.build_checkpoint_manager(&dag_metadata, cluster_descriptor.borrow_mut());
        ck_manager.run_align_task();
        info!("start CheckpointManager align task");

        self.web_serve(cluster_descriptor.borrow_mut(), ck_manager, dag_metadata);
        info!(
            "serve coordinator web ui {}",
            &cluster_descriptor.coordinator_manager.coordinator_address
        );

        self.resource_manager
            .prepare(&self.context, &cluster_descriptor);
        info!("ResourceManager prepared");

        self.gauge_startup(&cluster_descriptor);

        // loop restart all tasks when some task is failure
        loop {
            self.gauge_startup_number(cluster_descriptor.borrow_mut());

            // save metadata to storage
            self.save_metadata(&cluster_descriptor);
            info!("save metadata to storage");

            // allocate all worker's resources
            let worker_task_ids = self.allocate_worker();
            info!("allocate workers success");

            // blocking util all worker's status is `Register` status
            self.waiting_worker_status_fine();
            info!("all worker status is fine");

            // heartbeat check. blocking util heartbeat timeout
            let heartbeat_result =
                heart_beat_manager::start_heartbeat_timer(self.metadata_storage_mode.clone());
            info!("heartbeat has interrupted");

            // heartbeat timeout and stop all worker's tasks
            self.stop_all_worker_tasks(worker_task_ids);
            info!("stop all workers");

            // clear metadata from storage
            self.clear_metadata();
            info!("clear metadata from storage");

            if let HeartbeatResult::End = heartbeat_result {
                return Ok(());
            }
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
        application_properties: &Properties,
    ) -> ClusterDescriptor {
        let cluster_descriptor = build_cluster_descriptor(
            self.stream_env.application_name.as_str(),
            dag_manager,
            application_properties,
            &self.context,
        );
        // let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        // loop_save_job_descriptor(metadata_storage.borrow_mut(), job_descriptor.clone());
        cluster_descriptor
    }

    fn save_metadata(&self, cluster_descriptor: &ClusterDescriptor) {
        let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        loop_save_cluster_descriptor(metadata_storage.borrow_mut(), cluster_descriptor.clone());
    }

    fn clear_metadata(&self) {
        let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        loop_delete_cluster_descriptor(metadata_storage.borrow_mut());
    }

    fn build_checkpoint_manager(
        &self,
        dag_manager: &DagMetadata,
        cluster_descriptor: &mut ClusterDescriptor,
    ) -> CheckpointManager {
        let mut ck_manager = CheckpointManager::new(dag_manager, &self.context, cluster_descriptor);
        let operator_checkpoints = ck_manager.load().expect("load checkpoints error");
        if operator_checkpoints.len() == 0 {
            return ck_manager;
        }

        for task_manager_descriptor in &mut cluster_descriptor.worker_managers {
            for task_descriptor in &mut task_manager_descriptor.task_descriptors {
                let task_number = task_descriptor.task_id.task_number;
                for operator in &mut task_descriptor.operators {
                    let cks = operator_checkpoints.get(&operator.operator_id).unwrap();
                    if cks.len() == 0 {
                        debug!("operator {:?} checkpoint not found", operator.operator_id);
                        continue;
                    }

                    let ck = cks
                        .iter()
                        .find(|ck| ck.task_id.task_number == task_number)
                        .unwrap();
                    operator.checkpoint_id = ck.checkpoint_id;
                    operator.checkpoint_handle = Some(CheckpointHandle {
                        handle: ck.handle.handle.clone(),
                    });
                    info!("operator {:?} checkpoint loaded", operator);
                }
            }
        }

        ck_manager
    }

    fn web_serve(
        &self,
        cluster_descriptor: &mut ClusterDescriptor,
        checkpoint_manager: CheckpointManager,
        dag_metadata: DagMetadata,
    ) {
        let context = self.context.clone();
        let metadata_storage_mode = self.metadata_storage_mode.clone();

        let address = web_launch(
            context,
            metadata_storage_mode,
            checkpoint_manager,
            dag_metadata,
        );
        cluster_descriptor.coordinator_manager.coordinator_address = address;
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

            let job_descriptor = loop_read_cluster_descriptor(&metadata_storage);

            let unregister_worker = job_descriptor
                .worker_managers
                .iter()
                .find(|x| x.task_status.ne(&TaskManagerStatus::Registered));

            if unregister_worker.is_none() {
                loop_update_application_status(
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

    fn gauge_startup(&self, cluster_descriptor: &ClusterDescriptor) {
        let coordinator_manager = &cluster_descriptor.coordinator_manager;

        let uptime = Arc::new(AtomicI64::new(coordinator_manager.uptime as i64));
        register_gauge("uptime", vec![], uptime);

        let v_cores = Arc::new(AtomicI64::new(coordinator_manager.v_cores as i64));
        register_gauge("v_cores", vec![], v_cores);

        let memory_mb = Arc::new(AtomicI64::new(coordinator_manager.memory_mb as i64));
        register_gauge("memory_mb", vec![], memory_mb);

        let num_task_managers =
            Arc::new(AtomicI64::new(coordinator_manager.num_task_managers as i64));
        register_gauge("num_task_managers", vec![], num_task_managers);
    }

    fn gauge_startup_number(&self, cluster_descriptor: &mut ClusterDescriptor) {
        cluster_descriptor.coordinator_manager.startup_number += 1;
        self.startup_number.store(
            cluster_descriptor.coordinator_manager.startup_number as i64,
            Ordering::Relaxed,
        );
    }
}
