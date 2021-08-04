use std::convert::TryFrom;
use std::sync::Arc;

use crate::core::checkpoint::CheckpointHandle;
use crate::core::env::{StreamApp, StreamExecutionEnvironment};
use crate::core::function::InputSplit;
use crate::core::properties::Properties;
use crate::core::runtime::{CheckpointId, OperatorId, TaskId};
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
pub struct OperatorDescriptor {
    pub operator_id: OperatorId,
    pub checkpoint_id: CheckpointId,
    pub completed_checkpoint_id: Option<CheckpointId>,
    pub checkpoint_handle: Option<CheckpointHandle>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskDescriptor {
    pub task_id: TaskId,
    pub operators: Vec<OperatorDescriptor>,
    pub input_split: InputSplit,
    pub daemon: bool,
    pub thread_id: String,
    /// mark the task is stopped status
    pub stopped: bool,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq)]
pub enum ManagerStatus {
    /// Waiting for the TaskManager register
    Pending = 0,
    /// TaskManager has registered
    Registered = 1,
    /// TaskManager lost and try to recreate a new TaskManager
    Migration = 2,
    /// One of non-daemon Tasks terminated
    Terminating = 3,
    /// All Tasks terminated
    Terminated = 4,
}

impl ManagerStatus {
    pub fn is_terminating(&self) -> bool {
        match self {
            ManagerStatus::Terminating => true,
            _ => false,
        }
    }

    pub fn is_terminated(&self) -> bool {
        match self {
            ManagerStatus::Terminated => true,
            _ => false,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum HeartBeatStatus {
    Ok,
    Panic,
    End,
}

impl std::fmt::Display for HeartBeatStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HeartBeatStatus::Ok => write!(f, "ok"),
            HeartBeatStatus::Panic => write!(f, "panic"),
            HeartBeatStatus::End => write!(f, "end"),
        }
    }
}

impl<'a> TryFrom<&'a str> for HeartBeatStatus {
    type Error = anyhow::Error;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        match value {
            "ok" => Ok(HeartBeatStatus::Ok),
            "panic" => Ok(HeartBeatStatus::Panic),
            "end" => Ok(HeartBeatStatus::End),
            _ => Err(anyhow!("unrecognized status: {}", value)),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerManagerDescriptor {
    pub task_status: ManagerStatus,
    pub latest_heart_beat_ts: u64,
    pub latest_heart_beat_status: HeartBeatStatus,
    pub task_manager_id: String,
    pub task_manager_address: String,
    pub metrics_address: String,
    pub web_address: String,
    /// job tasks map: <job_id, Vec<TaskDescriptor>>
    pub task_descriptors: Vec<TaskDescriptor>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CoordinatorManagerDescriptor {
    /// `rlink` compile version
    pub version: String,
    pub application_id: String,
    pub application_properties: Properties,
    // todo rename to web_address
    pub coordinator_address: String,
    pub metrics_address: String,
    pub coordinator_status: ManagerStatus,
    pub v_cores: u32,
    pub memory_mb: u32,
    pub num_task_managers: u32,
    pub uptime: u64,
    pub startup_number: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ClusterDescriptor {
    pub coordinator_manager: CoordinatorManagerDescriptor,
    pub worker_managers: Vec<WorkerManagerDescriptor>,
}

impl ClusterDescriptor {
    pub fn get_worker_manager(&self, task_id: &TaskId) -> Option<&WorkerManagerDescriptor> {
        self.worker_managers
            .iter()
            .find(|worker_manager_descriptor| {
                worker_manager_descriptor
                    .task_descriptors
                    .iter()
                    .find(|task_descriptor| task_descriptor.task_id.eq(task_id))
                    .is_some()
            })
    }

    pub fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
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
