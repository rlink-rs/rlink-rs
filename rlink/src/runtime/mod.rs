use std::convert::TryFrom;
use std::sync::Arc;

use crate::api::checkpoint::CheckpointHandle;
use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::api::function::InputSplit;
use crate::api::properties::Properties;
use crate::api::runtime::{CheckpointId, OperatorId, TaskId};
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
    pub operator_ids: Vec<OperatorId>,
    pub input_split: InputSplit,
    pub checkpoint_id: CheckpointId,
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

impl ApplicationDescriptor {
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

pub fn run<S>(stream_env: StreamExecutionEnvironment, stream_app: S) -> anyhow::Result<()>
where
    S: StreamApp + 'static,
{
    panic_notify();

    let context = context::Context::parse_node_arg(stream_env.application_name.as_str())?;
    info!("Context: {:?}", context);

    cluster::run_task(Arc::new(context), stream_env, stream_app)
}
