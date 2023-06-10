use std::ops::Deref;
use std::sync::Arc;

use rlink_core::cluster::{ManagerType, TaskResourceInfo};
use rlink_core::context::Args;
use rlink_core::ClusterDescriptor;

use crate::deployment::{Resource, TResourceManager};
use crate::env::ApplicationEnv;
use crate::runtime::cluster;

#[derive(Clone)]
pub(crate) struct LocalResourceManager {
    args: Arc<Args>,
    cluster_descriptor: Option<ClusterDescriptor>,
}

impl LocalResourceManager {
    pub fn new(args: Arc<Args>) -> Self {
        LocalResourceManager {
            args,
            cluster_descriptor: None,
        }
    }
}

#[async_trait]
impl TResourceManager for LocalResourceManager {
    fn prepare(&mut self, _context: &Args, cluster_descriptor: &ClusterDescriptor) {
        self.cluster_descriptor = Some(cluster_descriptor.clone());
    }

    async fn worker_allocate(
        &self,
        env: Arc<ApplicationEnv>,
    ) -> anyhow::Result<Vec<TaskResourceInfo>> {
        let cluster_descriptor = self.cluster_descriptor.as_ref().unwrap();
        for task_manager_descriptor in &cluster_descriptor.worker_managers {
            let resource = Resource::new(
                cluster_descriptor.coordinator_manager.memory_mb,
                cluster_descriptor.coordinator_manager.v_cores,
            );
            info!(
                "TaskManager(id={}) allocate resource cpu:{}, memory:{}",
                task_manager_descriptor.task_manager_id, resource.cpu_cores, resource.memory
            );

            let mut args_clone = self.args.deref().clone();
            args_clone.manager_type = ManagerType::Worker;
            args_clone.task_manager_id = task_manager_descriptor.task_manager_id.clone();
            args_clone.coordinator_address =
                cluster_descriptor.coordinator_manager.web_address.clone();

            let dag_manager_clone = env.clone();
            tokio::spawn(async move {
                match cluster::run_task(Arc::new(args_clone), dag_manager_clone).await {
                    Ok(_) => {
                        info!("allocate local worker");
                    }
                    Err(e) => {
                        panic!("TaskManager error. {}", e)
                    }
                }
            });
        }

        Ok(Vec::new())
    }

    async fn stop_workers(&self, _task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        Ok(())
    }
}
