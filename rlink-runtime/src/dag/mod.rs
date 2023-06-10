//! DAG builder
//! stream_graph -> job_graph -> execution_graph

pub(crate) mod dag_manager;
pub(crate) mod execution_graph;
pub(crate) mod job_graph;
// pub(crate) mod metadata;
pub(crate) mod physic_graph;
// pub(crate) mod utils;

use std::fmt::Debug;

use rlink_core::TaskId;
use thiserror::Error;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct TaskInstance {
    pub task_id: TaskId,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct WorkerManagerInstance {
    /// build by self, format `format!("task_manager_{}", index)`
    pub worker_manager_id: String,
    /// task instances
    pub task_instances: Vec<TaskInstance>,
}

#[derive(Error, Debug)]
pub enum DagError {
    #[error("DAG wold cycle")]
    WouldCycle,
    #[error("job parallelism not found")]
    JobParallelismNotFound,
    #[error(transparent)]
    OtherApiError(#[from] rlink_core::error::Error),
}
