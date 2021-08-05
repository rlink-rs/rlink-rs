use std::convert::TryFrom;
use std::sync::Arc;

use crate::core::env::{StreamApp, StreamExecutionEnvironment};
use crate::core::runtime::{HeartBeatStatus, TaskId};
use crate::utils::panic::panic_notify;

pub mod cluster;
pub mod context;
pub mod coordinator;
pub mod logger;
pub mod timer;
pub mod worker;

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum ClusterMode {
    Local = 0,
    Standalone = 1,
    YARN = 2,
    Kubernetes = 3,
}

impl<'a> TryFrom<&'a str> for ClusterMode {
    type Error = anyhow::Error;

    fn try_from(mode_str: &'a str) -> Result<Self, Self::Error> {
        let mode_str = mode_str.to_ascii_lowercase();
        match mode_str.as_str() {
            "" => Ok(ClusterMode::Local),
            "local" => Ok(ClusterMode::Local),
            "standalone" => Ok(ClusterMode::Standalone),
            "yarn" => Ok(ClusterMode::YARN),
            "kubernetes" => Ok(ClusterMode::Kubernetes),
            _ => Err(anyhow!("Unsupported mode {}", mode_str)),
        }
    }
}

impl std::fmt::Display for ClusterMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterMode::Local => write!(f, "Local"),
            ClusterMode::Standalone => write!(f, "Standalone"),
            ClusterMode::YARN => write!(f, "Yarn"),
            ClusterMode::Kubernetes => write!(f, "Kubernetes"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum ManagerType {
    /// Job Manager activity
    Coordinator = 1,
    /// Task Manager
    Worker = 3,
}

impl<'a> TryFrom<&'a str> for ManagerType {
    type Error = anyhow::Error;

    fn try_from(mode_str: &'a str) -> Result<Self, Self::Error> {
        let mode_str = mode_str.to_ascii_lowercase();
        match mode_str.as_str() {
            "coordinator" => Ok(ManagerType::Coordinator),
            "worker" => Ok(ManagerType::Worker),
            _ => Err(anyhow!("Unsupported mode {}", mode_str)),
        }
    }
}

impl std::fmt::Display for ManagerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManagerType::Coordinator => write!(f, "Coordinator"),
            ManagerType::Worker => write!(f, "Worker"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum HeartbeatItem {
    WorkerManagerAddress(String),
    WorkerManagerWebAddress(String),
    MetricsAddress(String),
    HeartBeatStatus(HeartBeatStatus),
    TaskThreadId { task_id: TaskId, thread_id: u64 },
    TaskEnd { task_id: TaskId },
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct HeartbeatRequest {
    pub task_manager_id: String,
    pub change_items: Vec<HeartbeatItem>,
}

pub fn run<S>(stream_env: StreamExecutionEnvironment, stream_app: S) -> anyhow::Result<()>
where
    S: StreamApp + 'static,
{
    panic_notify();

    let context = context::Context::parse_node_arg()?;
    info!("Context: {:?}", context);

    cluster::run_task(Arc::new(context), stream_env, stream_app)
}
