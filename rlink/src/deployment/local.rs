use std::ops::Deref;
use std::sync::Arc;

use crate::api::cluster::TaskResourceInfo;
use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::deployment::{Resource, TResourceManager};
use crate::runtime::context::Context;
use crate::runtime::{cluster, ClusterDescriptor, ManagerType};

#[derive(Clone, Debug)]
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

impl TResourceManager for LocalResourceManager {
    fn prepare(&mut self, _context: &Context, cluster_descriptor: &ClusterDescriptor) {
        self.cluster_descriptor = Some(cluster_descriptor.clone());
    }

    fn worker_allocate<S>(
        &self,
        stream_app: &S,
        stream_env: &StreamExecutionEnvironment,
    ) -> anyhow::Result<Vec<TaskResourceInfo>>
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
            context_clone.coordinator_address = cluster_descriptor
                .coordinator_manager
                .coordinator_address
                .clone();

            let stream_app_clone = stream_app.clone();
            let application_name = stream_env.application_name.clone();
            std::thread::Builder::new()
                .name(format!(
                    "TaskManager(id={})",
                    &task_manager_descriptor.task_manager_id
                ))
                .spawn(move || {
                    let stream_env = StreamExecutionEnvironment::new(application_name);
                    match cluster::run_task(Arc::new(context_clone), stream_env, stream_app_clone) {
                        Ok(_) => {}
                        Err(e) => {
                            panic!("TaskManager error. {}", e)
                        }
                    }
                })
                .unwrap();
        }

        Ok(Vec::new())
    }

    fn stop_workers(&self, _task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        Ok(())
    }
}
