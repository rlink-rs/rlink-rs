use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::context::Args;
use crate::{ClusterDescriptor, JobId, TaskDescriptor, TaskId};

#[async_trait]
pub trait TaskContext: Send + Sync {
    fn context(&self) -> Arc<Args>;

    fn cluster_descriptor(&self) -> Arc<ClusterDescriptor>;

    fn task_descriptor(&self) -> Arc<TaskDescriptor>;

    fn job_id(&self) -> JobId;

    fn task_id(&self) -> TaskId;

    fn parent_jobs(&self) -> Vec<(&JobNode, &JobEdge)>;

    fn child_jobs(&self) -> Vec<(&JobNode, &JobEdge)>;

    fn parent_executions(&self, task_id: TaskId) -> Vec<(&ExecutionNode, &ExecutionEdge)>;

    fn child_executions(&self, task_id: TaskId) -> Vec<(&ExecutionNode, &ExecutionEdge)>;

    async fn create_job(&self, job_id: JobId) -> Box<dyn Job>;
}

// #[derive(Clone, Debug)]
// pub struct WorkerTaskContext {
//     #[allow(unused)]
//     pub context: Arc<Context>,
//     pub cluster_descriptor: Arc<ClusterDescriptor>,
//     pub task_descriptor: TaskDescriptor,
// }
//
// impl WorkerTaskContext {
//     pub fn new(
//         context: Arc<Context>,
//         cluster_descriptor: Arc<ClusterDescriptor>,
//         task_descriptor: TaskDescriptor,
//
//     ) -> Self {
//         Self {
//             context,
//             cluster_descriptor,
//             task_descriptor,
//         }
//     }
//
//     #[allow(unused)]
//     pub fn context(&self) -> Arc<Context> {
//         self.context.clone()
//     }
//
//     pub fn cluster_descriptor(&self) -> Arc<ClusterDescriptor> {
//         self.cluster_descriptor.clone()
//     }
//
//     #[allow(unused)]
//     pub fn task_descriptor(&self) -> &TaskDescriptor {
//         &self.task_descriptor
//     }
// }

#[async_trait]
pub trait Job
where
    Self: Send + Sync + Any,
{
    async fn open(&mut self, context: &Box<dyn TaskContext>) -> anyhow::Result<()>;
    async fn run(&mut self);
    async fn close(&mut self) -> anyhow::Result<()>;
}

#[async_trait]
pub trait JobFactory: Send + Sync + Debug {
    async fn create(&mut self, job_id: JobId) -> Box<dyn Job>;
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum JobEdge {
    /// Forward
    Forward = 1,
    /// Hash
    ReBalance = 2,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobNode {
    pub job_id: JobId,
    pub parallelism: u16,
}

impl JobNode {
    pub fn new(job_id: JobId, parallelism: u16) -> Self {
        Self {
            job_id,
            parallelism,
        }
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ExecutionEdge {
    /// Forward
    Memory = 1,
    /// Hash
    Network = 2,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ExecutionNode {
    pub task_id: TaskId,
    // pub daemon: bool,
}
