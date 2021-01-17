use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterConfig {
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

pub fn load_config(path: PathBuf) -> ClusterConfig {
    let context = read_config_from_path(path).expect("read Cluster config error");
    serde_yaml::from_str(&context).expect("parse Cluster config error")
}

pub fn read_config_from_path(path: PathBuf) -> Result<String, std::io::Error> {
    let mut file = File::open(path)?;
    let mut buffer = String::new();
    file.read_to_string(&mut buffer)?;
    Ok(buffer)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskResourceInfo {
    /// for standalone
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub task_manager_address: String,

    /// for yarn
    #[serde(default)]
    pub task_manager_id: String,
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
            task_id,
            task_manager_address,
            task_manager_id,
            resource_info,
        }
    }

    pub fn get_task_id(&self) -> &str {
        self.resource_info
            .get("task_id")
            .map(|x| x.as_str())
            .unwrap_or(self.task_id.as_str())
    }

    pub fn get_task_manager_address(&self) -> &str {
        self.resource_info
            .get("task_manager_address")
            .map(|x| x.as_str())
            .unwrap_or(self.task_manager_address.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteRequest {
    pub executable_file: String,
    pub args: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchExecuteRequest {
    // pub executable_file: String,
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
}

#[cfg(test)]
mod tests {
    use crate::api::cluster::{ClusterConfig, MetadataStorageType};

    #[test]
    pub fn ser_cluster_config_test() {
        let config = ClusterConfig {
            application_manager_address: vec!["addr_a".to_string(), "addr_b".to_string()],
            metadata_storage: MetadataStorageType::Memory,
            task_manager_bind_ip: "ip".to_string(),
            task_manager_work_dir: "dir".to_string(),
        };

        let yaml = serde_yaml::to_string(&config).unwrap();
        println!("{}", yaml);
    }
}
