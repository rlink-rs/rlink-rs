use crate::api::cluster::TaskResourceInfo;
use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::deployment::{Resource, TResourceManager};
use crate::runtime::context::Context;
use crate::runtime::{cluster, ApplicationDescriptor, ManagerType};

#[derive(Clone, Debug)]
pub(crate) struct LocalResourceManager {
    context: Context,
    application_descriptor: Option<ApplicationDescriptor>,
}

impl LocalResourceManager {
    pub fn new(context: Context) -> Self {
        LocalResourceManager {
            context,
            application_descriptor: None,
        }
    }
}

impl TResourceManager for LocalResourceManager {
    fn prepare(&mut self, _context: &Context, job_descriptor: &ApplicationDescriptor) {
        self.application_descriptor = Some(job_descriptor.clone());
    }

    fn worker_allocate<S>(
        &self,
        stream_app: &S,
        stream_env: &StreamExecutionEnvironment,
    ) -> anyhow::Result<Vec<TaskResourceInfo>>
    where
        S: StreamApp + 'static,
    {
        let application_descriptor = self.application_descriptor.as_ref().unwrap();
        for task_manager_descriptor in &application_descriptor.worker_managers {
            let resource = Resource::new(
                task_manager_descriptor.physical_memory,
                task_manager_descriptor.cpu_cores,
            );
            info!(
                "TaskManager(id={}) allocate resource cpu:{}, memory:{}",
                task_manager_descriptor.task_manager_id, resource.cpu_cores, resource.memory
            );

            let mut context_clone = self.context.clone();
            context_clone.manager_type = ManagerType::Worker;
            context_clone.task_manager_id = task_manager_descriptor.task_manager_id.clone();
            context_clone.coordinator_address = application_descriptor
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
                    match cluster::run_task(context_clone, stream_env, stream_app_clone) {
                        Ok(_) => {}
                        Err(e) => {
                            panic!(format!("TaskManager error. {}", e))
                        }
                    }
                })
                .unwrap();
        }

        Ok(Vec::new())
    }

    fn stop_workers(&self, _task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        unimplemented!()
    }
}
