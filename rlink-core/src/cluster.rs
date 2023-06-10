use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub enum ClusterMode {
    Local = 0,
    Standalone = 1,
    YARN = 2,
    Kubernetes = 3,
}

impl ClusterMode {
    pub fn is_local(&self) -> bool {
        match self {
            ClusterMode::Local => true,
            _ => false,
        }
    }
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

#[atomic_enum]
#[derive(Serialize, Deserialize, PartialEq)]
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

impl Default for ManagerStatus {
    fn default() -> Self {
        ManagerStatus::Pending
    }
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

/// Metadata(`ClusterDescriptor`) storage type
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "param")]
pub enum MetadataStorageType {
    Memory,
}

impl std::fmt::Display for MetadataStorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataStorageType::Memory => write!(f, "Memory"),
        }
    }
}

/// Cluster config, for communication with TaskManager under standalone
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// JobManager address, can specify multiple ones
    pub application_manager_address: Vec<String>,

    /// metadata storage mode
    pub metadata_storage: MetadataStorageType,

    pub task_manager_bind_ip: String,
    pub task_manager_work_dir: String,
}

impl ClusterConfig {
    pub fn new_local() -> Self {
        ClusterConfig {
            application_manager_address: Vec::new(),
            metadata_storage: MetadataStorageType::Memory,

            task_manager_bind_ip: "".to_string(),
            task_manager_work_dir: "./".to_string(),
        }
    }
}

/// load json config form path
pub fn load_config(path: PathBuf) -> anyhow::Result<ClusterConfig> {
    let context =
        read_config_from_path(path).map_err(|e| anyhow!("read Cluster config error {}", e))?;
    serde_yaml::from_str(&context).map_err(|e| anyhow!("parse Cluster config error {}", e))
}

/// load text config form path
pub fn read_config_from_path(path: PathBuf) -> Result<String, std::io::Error> {
    let mut file = File::open(path)?;
    let mut buffer = String::new();
    file.read_to_string(&mut buffer)?;
    Ok(buffer)
}

/// Describes a task resource info
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResourceInfo {
    /// The TaskManager's id for running the task
    #[serde(default)]
    pub task_manager_id: String,
    /// task's run arguments
    #[serde(default)]
    pub resource_info: HashMap<String, String>,
}

impl TaskResourceInfo {
    /// for standalone
    pub fn new(task_id: String, task_manager_address: String, task_manager_id: String) -> Self {
        let mut resource_info = HashMap::new();
        resource_info.insert("task_id".to_string(), task_id.clone());
        resource_info.insert(
            "task_manager_address".to_string(),
            task_manager_address.clone(),
        );

        TaskResourceInfo {
            task_manager_id,
            resource_info,
        }
    }

    pub fn task_id(&self) -> Option<&String> {
        self.resource_info.get("task_id")
    }

    pub fn task_manager_address(&self) -> Option<&String> {
        self.resource_info.get("task_manager_address")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteRequest {
    pub executable_file: String,
    pub args: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchExecuteRequest {
    pub batch_args: Vec<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum ResponseCode {
    OK,
    ERR(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StdResponse<T> {
    pub code: ResponseCode,
    pub data: Option<T>,
}

impl<T> StdResponse<T> {
    pub fn new(code: ResponseCode, data: Option<T>) -> Self {
        StdResponse { code, data }
    }

    pub fn ok(data: Option<T>) -> Self {
        StdResponse {
            code: ResponseCode::OK,
            data,
        }
    }

    pub fn err<E: ToString>(err_msg: E) -> Self {
        StdResponse {
            code: ResponseCode::ERR(err_msg.to_string()),
            data: None,
        }
    }
}

impl<T> Into<StdResponse<T>> for anyhow::Result<T> {
    fn into(self) -> StdResponse<T> {
        match self {
            Ok(t) => StdResponse::ok(Some(t)),
            Err(e) => StdResponse::err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::{ClusterConfig, MetadataStorageType};

    #[test]
    pub fn ser_cluster_config_test() {
        let config = ClusterConfig {
            application_manager_address: vec![
                "http://0.0.0.0:8370".to_string(),
                "http://0.0.0.0:8370".to_string(),
            ],
            metadata_storage: MetadataStorageType::Memory,
            task_manager_bind_ip: "0.0.0.0".to_string(),
            task_manager_work_dir: "/data/rlink/application".to_string(),
        };

        let yaml = serde_yaml::to_string(&config).unwrap();
        println!("{}", yaml);

        let config1: ClusterConfig = serde_yaml::from_str(yaml.as_str()).unwrap();

        assert!(config.metadata_storage.eq(&config1.metadata_storage));
    }
}
