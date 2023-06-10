use std::borrow::BorrowMut;
use std::sync::Arc;
use std::time::Duration;

use metrics::Gauge;
use rlink_core::cluster::{ManagerStatus, MetadataStorageType, TaskResourceInfo};
use rlink_core::context::Args;
use rlink_core::metrics::register_gauge;
use rlink_core::ClusterDescriptor;

use crate::deployment::TResourceManager;
use crate::env::ApplicationEnv;
use crate::runtime::coordinator::heart_beat_manager::HeartbeatResult;
use crate::runtime::coordinator::task_distribution::build_cluster_descriptor;
use crate::runtime::coordinator::web_server::web_launch;
use crate::storage::metadata::{
    loop_read_cluster_descriptor, loop_save_cluster_descriptor, loop_update_application_status,
    MetadataStorage,
};
use crate::utils::date_time::timestamp_str;

pub mod heart_beat_manager;
pub mod task_distribution;
pub mod web_server;

pub(crate) struct CoordinatorTask<R>
where
    R: TResourceManager + 'static,
{
    args: Arc<Args>,
    env: Arc<ApplicationEnv>,
    metadata_storage_mode: MetadataStorageType,
    resource_manager: R,
    startup_number: Gauge,
}

impl<R> CoordinatorTask<R>
where
    R: TResourceManager + 'static,
{
    pub fn new(args: Arc<Args>, env: Arc<ApplicationEnv>, resource_manager: R) -> Self {
        let metadata_storage_mode = args.cluster_config.metadata_storage.clone();

        let startup_number = register_gauge("startup_number", vec![]);

        CoordinatorTask {
            args,
            env,
            metadata_storage_mode,
            resource_manager,
            startup_number,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        info!("coordinator start with mode {}", self.args.manager_type);

        let mut cluster_descriptor = self.build_metadata();
        debug!("ApplicationDescriptor : {}", cluster_descriptor.to_string());

        self.web_serve(cluster_descriptor.borrow_mut()).await;
        info!(
            "serve coordinator web ui {}",
            &cluster_descriptor.coordinator_manager.web_address
        );

        self.resource_manager
            .prepare(&self.args, &cluster_descriptor);
        info!("ResourceManager prepared");

        self.gauge_startup(&cluster_descriptor);

        // loop restart all tasks when some task is failure
        loop {
            self.gauge_startup_number(cluster_descriptor.borrow_mut());

            // save metadata to storage
            self.save_metadata(&cluster_descriptor).await;
            info!("save metadata to storage");

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

    fn build_metadata(&self) -> ClusterDescriptor {
        let cluster_descriptor = build_cluster_descriptor(
            &self.env.dag_manager(),
            self.env.properties(),
            self.args.clone(),
        );
        // let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        // loop_save_job_descriptor(metadata_storage.borrow_mut(), job_descriptor.clone());
        cluster_descriptor
    }

    async fn save_metadata(&self, cluster_descriptor: &ClusterDescriptor) {
        let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        loop_save_cluster_descriptor(metadata_storage.borrow_mut(), cluster_descriptor.clone())
            .await;
    }

    async fn web_serve(&self, cluster_descriptor: &mut ClusterDescriptor) {
        let context = self.args.clone();
        let metadata_storage_mode = self.metadata_storage_mode.clone();

        let address = web_launch(context, metadata_storage_mode, self.env.dag_manager()).await;
        cluster_descriptor.coordinator_manager.web_address = address;
    }

    async fn allocate_worker(&self) -> Vec<TaskResourceInfo> {
        self.resource_manager
            .worker_allocate(self.env.clone())
            .await
            .expect("try allocate worker error")
    }

    async fn waiting_worker_status_fine(&self) {
        let mut metadata_storage = MetadataStorage::new(&self.metadata_storage_mode);
        loop {
            info!("waiting all workers status fine...");

            let cluster_descriptor = loop_read_cluster_descriptor(&metadata_storage).await;

            let unregister_worker = cluster_descriptor
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
                cluster_descriptor.worker_managers.iter().for_each(|tm| {
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
        uptime.set(coordinator_manager.uptime as f64);

        let v_cores = register_gauge("v_cores", vec![]);
        v_cores.set(coordinator_manager.v_cores as f64);

        let memory_mb = register_gauge("memory_mb", vec![]);
        memory_mb.set(coordinator_manager.memory_mb as f64);

        let num_task_managers = register_gauge("num_task_managers", vec![]);
        num_task_managers.set(coordinator_manager.num_task_managers as f64);
    }

    fn gauge_startup_number(&self, cluster_descriptor: &mut ClusterDescriptor) {
        cluster_descriptor.coordinator_manager.startup_number += 1;
        self.startup_number
            .set(cluster_descriptor.coordinator_manager.startup_number as f64);
    }
}
