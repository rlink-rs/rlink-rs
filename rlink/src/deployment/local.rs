use std::ops::Deref;
use std::sync::Arc;

use crate::core::cluster::TaskResourceInfo;
use crate::core::env::StreamApp;
use crate::core::runtime::ClusterDescriptor;
use crate::deployment::{Resource, TResourceManager};
use crate::runtime::context::Context;
use crate::runtime::{cluster, ManagerType};

#[derive(Clone)]
pub(crate) struct LocalResourceManager {
    context: Arc<Context>,
    cluster_descriptor: Option<ClusterDescriptor>,
}

impl LocalResourceManager {
    pub fn new(context: Arc<Context>) -> Self {
        LocalResourceManager {
            context,
            cluster_descriptor: None,
        }
    }
}

#[async_trait]
impl TResourceManager for LocalResourceManager {
    fn prepare(&mut self, _context: &Context, cluster_descriptor: &ClusterDescriptor) {
        self.cluster_descriptor = Some(cluster_descriptor.clone());
    }

    async fn worker_allocate<S>(&self, stream_app: &S) -> anyhow::Result<Vec<TaskResourceInfo>>
    where
        S: StreamApp + 'static,
    {
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

            let mut context_clone = self.context.deref().clone();
            context_clone.manager_type = ManagerType::Worker;
            context_clone.task_manager_id = task_manager_descriptor.task_manager_id.clone();
            context_clone.coordinator_address =
                cluster_descriptor.coordinator_manager.web_address.clone();

            let stream_app_clone = stream_app.clone();
            tokio::spawn(async move {
                match cluster::run_task(Arc::new(context_clone), stream_app_clone).await {
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
