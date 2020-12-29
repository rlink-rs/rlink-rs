use crate::api::cluster::TaskResourceInfo;
use crate::api::env::{StreamExecutionEnvironment, StreamJob};
use crate::deployment::{Resource, ResourceManager};
use crate::runtime::context::Context;
use crate::runtime::{cluster, JobDescriptor, ManagerType};

#[derive(Clone, Debug)]
pub(crate) struct LocalResourceManager {
    context: Context,
    job_descriptor: Option<JobDescriptor>,
}

impl LocalResourceManager {
    pub fn new(context: Context) -> Self {
        LocalResourceManager {
            context,
            job_descriptor: None,
        }
    }
}

impl ResourceManager for LocalResourceManager {
    fn prepare(&mut self, _context: &Context, job_descriptor: &JobDescriptor) {
        self.job_descriptor = Some(job_descriptor.clone());
    }

    fn worker_allocate<S>(
        &self,
        stream_job: &S,
        stream_env: &StreamExecutionEnvironment,
    ) -> anyhow::Result<Vec<TaskResourceInfo>>
    where
        S: StreamJob + 'static,
    {
        let job_descriptor = self.job_descriptor.as_ref().unwrap();
        for task_manager_descriptor in &job_descriptor.task_managers {
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
            context_clone.coordinator_address =
                job_descriptor.job_manager.coordinator_address.clone();

            let stream_job_clone = stream_job.clone();
            let stream_env_clone = stream_env.clone();
            std::thread::Builder::new()
                .name(format!(
                    "TaskManager(id={})",
                    &task_manager_descriptor.task_manager_id
                ))
                .spawn(move || {
                    cluster::run_task(context_clone, stream_env_clone, stream_job_clone);
                })
                .unwrap();
        }

        Ok(Vec::new())
    }

    fn stop_workers(&self, _task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        unimplemented!()
    }
}
