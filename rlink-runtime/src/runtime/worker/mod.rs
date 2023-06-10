use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use rlink_core::cluster::ManagerStatus;
use rlink_core::context::Args;
use rlink_core::{ClusterDescriptor, HeartBeatStatus, HeartbeatItem};
use tokio::task::JoinHandle;

use crate::env::ApplicationEnv;
use crate::runtime::worker::context::WorkerTaskContext;
use crate::runtime::worker::heart_beat::HeartbeatPublish;
use crate::runtime::worker::job_task::JobTask;
use crate::runtime::worker::web_server::web_launch;
use crate::storage::metadata::metadata_loader::MetadataLoader;
use crate::timer::{start_window_timer, WindowTimer};

pub mod context;
pub mod heart_beat;
pub mod job_task;
pub mod web_server;

pub struct WorkerTask {
    args: Arc<Args>,
    env: Arc<ApplicationEnv>,
}

impl WorkerTask {
    pub fn new(args: Arc<Args>, env: Arc<ApplicationEnv>) -> Self {
        Self { args, env }
    }

    pub(crate) async fn run(&self) -> anyhow::Result<()> {
        let metadata_loader = MetadataLoader::new(self.args.coordinator_address.as_str());

        let cluster_descriptor = metadata_loader.get_cluster_descriptor().await?;
        info!("preload `ClusterDescriptor`");

        let web_address = self.web_serve().await;
        info!("serve worker web ui {}", web_address);

        let window_timer = start_window_timer().await;
        info!("bootstrap window timer");

        let heartbeat_publish = self
            .start_heartbeat_task(&cluster_descriptor, web_address.as_str())
            .await;
        info!("start heartbeat timer and register worker to coordinator");

        let cluster_descriptor = self.waiting_all_task_manager_fine(&metadata_loader).await?;
        info!("all task manager is fine");

        let join_handles = self
            .run_tasks(
                cluster_descriptor.clone(),
                window_timer,
                heartbeat_publish.clone(),
                self.env.clone(),
            )
            .await;
        info!("all task has bootstrap");

        for join_handle in join_handles {
            join_handle.await.unwrap();
        }

        self.stop_heartbeat_timer(heartbeat_publish.deref()).await;
        info!("work end");

        Ok(())
    }

    async fn web_serve(&self) -> String {
        let address = web_launch(self.args.clone()).await;
        address
    }

    async fn start_heartbeat_task(
        &self,
        cluster_descriptor: &ClusterDescriptor,
        web_addr: &str,
    ) -> Arc<HeartbeatPublish> {
        let coordinator_address = cluster_descriptor.coordinator_manager.web_address.clone();
        let task_manager_id = self.args.task_manager_id.clone();

        let heartbeat_publish =
            HeartbeatPublish::new(coordinator_address.clone(), task_manager_id.clone()).await;

        let status = HeartbeatItem::WorkerAddrs {
            web_address: web_addr.to_string(),
        };
        heartbeat_publish.report(status).await;

        heartbeat_publish.start_heartbeat_timer().await;

        Arc::new(heartbeat_publish)
    }

    async fn stop_heartbeat_timer(&self, heartbeat_publish: &HeartbeatPublish) {
        let status = HeartbeatItem::HeartBeatStatus(HeartBeatStatus::End);
        heartbeat_publish.report(status).await;
    }

    async fn waiting_all_task_manager_fine(
        &self,
        metadata_loader: &MetadataLoader,
    ) -> anyhow::Result<Arc<ClusterDescriptor>> {
        let cluster_descriptor = self.waiting_all_task_manager_fine0(metadata_loader).await?;
        Ok(Arc::new(cluster_descriptor))
    }

    async fn waiting_all_task_manager_fine0(
        &self,
        metadata_loader: &MetadataLoader,
    ) -> anyhow::Result<ClusterDescriptor> {
        loop {
            let cluster_descriptor = metadata_loader.get_cluster_descriptor().await?;
            match cluster_descriptor.coordinator_manager.status {
                ManagerStatus::Registered => {
                    return Ok(cluster_descriptor);
                }
                _ => tokio::time::sleep(Duration::from_secs(2)).await,
            }
        }
    }

    async fn run_tasks(
        &self,
        cluster_descriptor: Arc<ClusterDescriptor>,
        window_timer: WindowTimer,
        heartbeat_publish: Arc<HeartbeatPublish>,
        env: Arc<ApplicationEnv>,
    ) -> Vec<JoinHandle<()>> {
        let task_manager_id = self.args.task_manager_id.as_str();
        // todo code optimization
        let task_manager_descriptors = cluster_descriptor
            .get_worker_manager_by_mid(task_manager_id)
            .unwrap();

        let mut join_handles = Vec::with_capacity(task_manager_descriptors.task_descriptors.len());
        for task_descriptor in &task_manager_descriptors.task_descriptors {
            let task_context = Box::new(WorkerTaskContext::new(
                self.args.clone(),
                cluster_descriptor.clone(),
                Arc::new(task_descriptor.clone()),
                env.clone(),
                window_timer.clone(),
                heartbeat_publish.clone(),
            ));

            let join_handle = JobTask::run_task(Arc::new(task_context)).await;
            join_handles.push(join_handle);
        }

        join_handles
    }
}
