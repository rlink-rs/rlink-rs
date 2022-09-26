use std::borrow::BorrowMut;
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use crate::core::checkpoint::CheckpointHandle;
use crate::core::cluster::MetadataStorageType;
use crate::core::cluster::TaskResourceInfo;
use crate::core::env::{StreamApp, StreamExecutionEnvironment};
use crate::core::properties::{InnerSystemProperties, Properties, SystemProperties};
use crate::core::runtime::{ClusterDescriptor, ManagerStatus};
use crate::dag::metadata::DagMetadata;
use crate::dag::DagManager;
use crate::deployment::TResourceManager;
use crate::metrics::metric::Gauge;
use crate::metrics::register_gauge;
use crate::runtime::context::Context;
use crate::runtime::coordinator::checkpoint_manager::CheckpointManager;
use crate::runtime::coordinator::heart_beat_manager::HeartbeatResult;
use crate::runtime::coordinator::task_distribution::build_cluster_descriptor;
use crate::runtime::coordinator::web_server::web_launch;
use crate::storage::metadata::{
    loop_read_cluster_descriptor, loop_save_cluster_descriptor, loop_update_application_status,
    MetadataStorage,
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
    // stream_env: StreamExecutionEnvironment,
    startup_number: Gauge,
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
        // stream_env: StreamExecutionEnvironment,
    ) -> Self {
        let metadata_storage_mode = context.cluster_config.metadata_storage.clone();

        let startup_number = register_gauge("startup_number", vec![]);

        CoordinatorTask {
            context,
            stream_app,
            metadata_storage_mode,
            resource_manager,
            // stream_env,
            startup_number,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!("coordinator start with mode {}", self.context.manager_type);

        let application_properties = self.prepare_properties().await;

        let dag_manager = {
            let mut stream_env = StreamExecutionEnvironment::new();
            self.stream_app
                .build_stream(&application_properties, stream_env.borrow_mut());

            let dag_manager = {
                let raw_stream_graph = stream_env.stream_manager.stream_graph.borrow();
                DagManager::try_from(raw_stream_graph.deref())?
            };
            dag_manager
        };
        info!("DagManager build success");

        let dag_metadata = DagMetadata::from(&dag_manager);
        debug!("DagMetadata: {}", dag_metadata.to_string());

        let mut cluster_descriptor = self.build_metadata(&dag_manager, &application_properties);
        debug!("ApplicationDescriptor : {}", cluster_descriptor.to_string());

        let ck_manager = self
            .build_checkpoint_manager(
                &dag_metadata,
                &application_properties,
                cluster_descriptor.borrow_mut(),
            )
            .await;
        info!("start CheckpointManager align task");

        self.web_serve(cluster_descriptor.borrow_mut(), ck_manager, dag_metadata)
            .await;
        info!(
            "serve coordinator web ui {}",
            &cluster_descriptor.coordinator_manager.web_address
        );

        self.resource_manager
            .prepare(&self.context, &cluster_descriptor);
        info!("ResourceManager prepared");

        self.gauge_startup(&cluster_descriptor);

        // loop restart all tasks when some task is failure
        loop {
            self.gauge_startup_number(cluster_descriptor.borrow_mut());

            // save metadata to storage
            self.save_metadata(&cluster_descriptor).await;
            info!("save metadata to storage");

            self.stream_app
                .pre_worker_startup(&cluster_descriptor)
                .await;
            info!("pre-worker startup event");

            // allocate all worker's resources
            let worker_task_ids = self.allocate_worker().await;
            info!("allocate workers success");

            // blocking util all worker's status is `Register` status
            self.waiting_worker_status_fine().await;
            info!("all worker status is fine");

            // heartbeat check. blocking util heartbeat timeout
            let heartbeat_result =
                heart_beat_manager::start_heartbeat_timer(self.metadata_storage_mode.clone()).await;
            info!("heartbeat timer has interrupted");

            // heartbeat timeout and stop all worker's tasks
            self.stop_all_worker_tasks(worker_task_ids).await;
            info!("stop all workers");

            if let HeartbeatResult::End = heartbeat_result {
                return Ok(());
            }
        }
    }

    async fn prepare_properties(&self) -> Properties {
        let mut application_properties = Properties::new();
        application_properties.set_cluster_mode(self.context.cluster_mode);

        self.stream_app
            .prepare_properties(application_properties.borrow_mut())
            .await;

        let mut keys: Vec<&str> = application_properties
            .as_map()
            .keys()
            .map(|x| x.as_str())
            .collect();
        keys.sort();

        for key in keys {
            let value = application_properties.as_map().get(key).unwrap();
            info!("properties key={}, value={}", key, value,);
        }

        application_properties
    }

    fn build_metadata(
        &mut self,
        dag_manager: &DagManager,
        application_properties: &Properties,
    ) -> ClusterDescriptor {
        let cluster_descriptor =
            build_cluster_descriptor(dag_manager, application_properties, &self.context);
        // let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        // loop_save_job_descriptor(metadata_storage.borrow_mut(), job_descriptor.clone());
        cluster_descriptor
    }

    async fn save_metadata(&self, cluster_descriptor: &ClusterDescriptor) {
        let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        loop_save_cluster_descriptor(metadata_storage.borrow_mut(), cluster_descriptor.clone())
            .await;
    }

    // fn clear_metadata(&self) {
    //     let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
    //     loop_delete_cluster_descriptor(metadata_storage.borrow_mut());
    // }

    async fn build_checkpoint_manager(
        &self,
        dag_manager: &DagMetadata,
        application_properties: &Properties,
        cluster_descriptor: &mut ClusterDescriptor,
    ) -> CheckpointManager {
        let checkpoint_ttl = application_properties
            .get_checkpoint_ttl()
            .unwrap_or_else(|_e| Duration::from_secs(1 * 60 * 60));

        let mut ck_manager = CheckpointManager::new(
            dag_manager,
            &self.context,
            cluster_descriptor,
            checkpoint_ttl,
        )
        .await;
        let operator_checkpoints = ck_manager.load().await.expect("load checkpoints error");
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

    async fn web_serve(
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
        )
        .await;
        cluster_descriptor.coordinator_manager.web_address = address;
    }

    async fn allocate_worker(&self) -> Vec<TaskResourceInfo> {
        self.resource_manager
            .worker_allocate(&self.stream_app)
            .await
            .expect("try allocate worker error")
    }

    async fn waiting_worker_status_fine(&self) {
        let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        loop {
            info!("waiting all workers status fine...");

            let job_descriptor = loop_read_cluster_descriptor(&metadata_storage).await;

            let unregister_worker = job_descriptor
                .worker_managers
                .iter()
                .find(|x| x.status.ne(&ManagerStatus::Registered));

            if unregister_worker.is_none() {
                loop_update_application_status(
                    metadata_storage.borrow_mut(),
                    ManagerStatus::Registered,
                )
                .await;
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

            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    async fn stop_all_worker_tasks(&self, worker_task_ids: Vec<TaskResourceInfo>) {
        // loop stop all workers util all are success
        loop {
            let rt = self
                .resource_manager
                .stop_workers(worker_task_ids.clone())
                .await;
            match rt {
                Ok(_) => {
                    break;
                }
                Err(e) => {
                    error!("try stop all workers error. {}", e);
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }

    fn gauge_startup(&self, cluster_descriptor: &ClusterDescriptor) {
        let coordinator_manager = &cluster_descriptor.coordinator_manager;

        let uptime = register_gauge("uptime", vec![]);
        uptime.store(coordinator_manager.uptime as i64);

        let v_cores = register_gauge("v_cores", vec![]);
        v_cores.store(coordinator_manager.v_cores as i64);

        let memory_mb = register_gauge("memory_mb", vec![]);
        memory_mb.store(coordinator_manager.memory_mb as i64);

        let num_task_managers = register_gauge("num_task_managers", vec![]);
        num_task_managers.store(coordinator_manager.num_task_managers as i64);
    }

    fn gauge_startup_number(&self, cluster_descriptor: &mut ClusterDescriptor) {
        cluster_descriptor.coordinator_manager.startup_number += 1;
        self.startup_number
            .store(cluster_descriptor.coordinator_manager.startup_number as i64);
    }
}
