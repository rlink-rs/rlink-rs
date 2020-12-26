use crate::api::checkpoint::CheckpointHandle;
use crate::api::env::{StreamExecutionEnvironment, StreamJob};
use crate::api::properties::Properties;
use crate::api::split::InputSplit;
use crate::utils::panic::panic_notify;
use serde::export::Formatter;
use std::collections::HashMap;
use std::fmt::Display;

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

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskDescriptor {
    pub task_id: String,
    /// task sequence number. each chain，task sequence number start at 0.
    pub task_number: u16,
    /// total number tasks in the chain.
    pub num_tasks: u16,
    pub chain_id: u32,
    pub dependency_chain_id: u32,
    pub follower_chain_id: u32,
    pub dependency_parallelism: u32,
    pub follower_parallelism: u32,
    pub input_split: InputSplit,
    pub checkpoint_id: u64,
    pub checkpoint_handle: Option<CheckpointHandle>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskManagerDescriptor {
    pub task_status: TaskManagerStatus,
    pub latest_heart_beat_ts: u64,
    pub task_manager_id: String,
    pub task_manager_address: String,
    pub metrics_address: String,
    pub cpu_cores: u32,
    pub physical_memory: u32,
    /// chain tasks map: <chain_id, Vec<TaskDescriptor>>
    pub chain_tasks: HashMap<u32, Vec<TaskDescriptor>>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct JobManagerDescriptor {
    pub job_id: String,
    pub job_name: String,
    pub job_properties: Properties,
    pub coordinator_address: String,
    pub job_status: TaskManagerStatus,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct JobDescriptor {
    pub job_manager: JobManagerDescriptor,
    pub task_managers: Vec<TaskManagerDescriptor>,
}

pub fn run<S>(stream_env: StreamExecutionEnvironment, stream_job: S)
where
    S: StreamJob + 'static,
{
    panic_notify();

    let context = context::Context::parse_node_arg(stream_env.job_name.as_str());
    info!("Context: {:?}", context);

    cluster::run_task(context, stream_env, stream_job);
}
