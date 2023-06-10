use std::sync::Arc;

use rlink_core::context::Args;
use rlink_core::job::{ExecutionEdge, ExecutionNode, Job, JobEdge, JobNode, TaskContext};
use rlink_core::{ClusterDescriptor, JobId, TaskDescriptor, TaskId};

use crate::env::ApplicationEnv;
use crate::runtime::worker::heart_beat::HeartbeatPublish;
use crate::timer::WindowTimer;

pub struct WorkerTaskContext {
    context: Arc<Args>,
    cluster_descriptor: Arc<ClusterDescriptor>,
    task_descriptor: Arc<TaskDescriptor>,
    env: Arc<ApplicationEnv>,

    window_timer: WindowTimer,
    heartbeat_publish: Arc<HeartbeatPublish>,
}

impl WorkerTaskContext {
    pub fn new(
        context: Arc<Args>,
        cluster_descriptor: Arc<ClusterDescriptor>,
        task_descriptor: Arc<TaskDescriptor>,
        env: Arc<ApplicationEnv>,
        window_timer: WindowTimer,
        heartbeat_publish: Arc<HeartbeatPublish>,
    ) -> Self {
        Self {
            context,
            cluster_descriptor,
            task_descriptor,
            env,
            window_timer,
            heartbeat_publish,
        }
    }
}

#[async_trait]
impl TaskContext for WorkerTaskContext {
    fn context(&self) -> Arc<Args> {
        self.context.clone()
    }

    fn cluster_descriptor(&self) -> Arc<ClusterDescriptor> {
        self.cluster_descriptor.clone()
    }

    fn task_descriptor(&self) -> Arc<TaskDescriptor> {
        self.task_descriptor.clone()
    }

    fn job_id(&self) -> JobId {
        self.task_id().job_id
    }

    fn task_id(&self) -> TaskId {
        self.task_descriptor.task_id
    }

    fn parent_jobs(&self) -> Vec<(&JobNode, &JobEdge)> {
        todo!()
    }

    fn child_jobs(&self) -> Vec<(&JobNode, &JobEdge)> {
        todo!()
    }

    fn parent_executions(&self, _task_id: TaskId) -> Vec<(&ExecutionNode, &ExecutionEdge)> {
        todo!()
    }

    fn child_executions(&self, _task_id: TaskId) -> Vec<(&ExecutionNode, &ExecutionEdge)> {
        todo!()
    }

    async fn create_job(&self, job_id: JobId) -> Box<dyn Job> {
        self.env.dag_manager().create_job(job_id).await
    }
}
