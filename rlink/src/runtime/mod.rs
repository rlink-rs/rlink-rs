use std::fmt::Display;

use serde::export::Formatter;

use crate::api::checkpoint::CheckpointHandle;
use crate::api::env::{StreamExecutionEnvironment, StreamJob};
use crate::api::function::InputSplit;
use crate::api::properties::Properties;
use crate::api::runtime::TaskId;
use crate::utils::panic::panic_notify;

pub mod cluster;
pub mod context;
pub mod coordinator;
pub mod logger;
pub mod worker;

pub type ChainId = u32;
pub type CheckpointId = u64;

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum ClusterMode {
    Local = 0,
    Standalone = 1,
    YARN = 2,
}

impl From<String> for ClusterMode {
    fn from(mode_str: String) -> Self {
        let mode_str = mode_str.to_ascii_lowercase();
        match mode_str.as_str() {
            "" => ClusterMode::Local,
            "local" => ClusterMode::Local,
            "standalone" => ClusterMode::Standalone,
            "yarn" => ClusterMode::YARN,
            _ => panic!(format!("Unsupported mode {}", mode_str)),
        }
    }
}

impl Display for ClusterMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterMode::Local => write!(f, "Local"),
            ClusterMode::Standalone => write!(f, "Standalone"),
            ClusterMode::YARN => write!(f, "Yarn"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum ManagerType {
    /// Job Manager activity
    Coordinator = 1,
    /// Job Manager standby
    Standby = 2,
    /// Task Manager
    Worker = 3,
}

impl From<String> for ManagerType {
    fn from(mode_str: String) -> Self {
        let mode_str = mode_str.to_ascii_lowercase();
        match mode_str.as_str() {
            "coordinator" => ManagerType::Coordinator,
            "standby" => ManagerType::Standby,
            "worker" => ManagerType::Worker,
            _ => panic!(format!("Unsupported mode {}", mode_str)),
        }
    }
}

impl Display for ManagerType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ManagerType::Coordinator => write!(f, "Coordinator"),
            ManagerType::Standby => write!(f, "Standby"),
            ManagerType::Worker => write!(f, "Worker"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq)]
pub enum TaskManagerStatus {
    /// waiting for the TaskManager register
    Pending = 0,
    /// TaskManager has registered
    Registered = 1,
    /// TaskManager lost and try to recreate a new TaskManager
    Migration = 2,
}

// todo rename to TaskDescriptor
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskDescriptor {
    pub task_id: TaskId,
    pub input_split: InputSplit,
    pub checkpoint_id: u64,
    pub checkpoint_handle: Option<CheckpointHandle>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerManagerDescriptor {
    pub task_status: TaskManagerStatus,
    pub latest_heart_beat_ts: u64,
    pub task_manager_id: String,
    pub task_manager_address: String,
    pub metrics_address: String,
    pub cpu_cores: u32,
    pub physical_memory: u32,
    /// job tasks map: <job_id, Vec<TaskDescriptor>>
    pub task_descriptors: Vec<TaskDescriptor>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CoordinatorManagerDescriptor {
    pub application_id: String,
    pub application_name: String,
    pub application_properties: Properties,
    pub coordinator_address: String,
    pub coordinator_status: TaskManagerStatus,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ApplicationDescriptor {
    pub coordinator_manager: CoordinatorManagerDescriptor,
    pub worker_managers: Vec<WorkerManagerDescriptor>,
}

pub fn run<S>(stream_env: StreamExecutionEnvironment, stream_job: S)
where
    S: StreamJob + 'static,
{
    panic_notify();

    let context = context::Context::parse_node_arg(stream_env.application_name.as_str());
    info!("Context: {:?}", context);

    cluster::run_task(context, stream_env, stream_job);
}
