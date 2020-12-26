use crate::runtime::CheckpointId;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "param")]
pub enum CheckpointBackend {
    Memory,
    MySql { endpoint: String },
}

impl Display for CheckpointBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckpointBackend::Memory => write!(f, "Memory"),
            CheckpointBackend::MySql { endpoint } => write!(f, "MySql{{endpoint={}}}", endpoint),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "param")]
pub enum KeyedStateBackend {
    Memory,
    // FsStateBackend(String),
    // RocksDBStateBackend(String),
}

impl Display for KeyedStateBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyedStateBackend::Memory => write!(f, "Memory"),
            // StateBackend::FsStateBackend(path) => write!(f, "FsStateBackend{{path={}}}", path),
            // KeyedStateBackend::RocksDBStateBackend(path) => {
            //     write!(f, "RocksDBStateBackend{{path={}}}", path)
            // }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "param")]
pub enum OperatorStateBackend {
    None,
    // DFSStateBackendWorkDir,
    // DFSStateBackend(String),
}

impl Display for OperatorStateBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OperatorStateBackend::None => write!(f, "None"),
            // StateBackend::FsStateBackend(path) => write!(f, "FsStateBackend{{path={}}}", path),
            // OperatorStateBackend::DFSStateBackendWorkDir => write!(f, "DFSStateBackendWorkDir"),
            // OperatorStateBackend::DFSStateBackend(path) => {
            //     write!(f, "DFSStateBackend{{path={}}}", path)
            // }
        }
    }
}

#[derive(Clone, Debug)]
pub struct StateValue {
    pub values: Vec<String>,
}

impl StateValue {
    pub fn new(values: Vec<String>) -> Self {
        StateValue { values }
    }
}

pub trait OperatorState: Debug {
    fn update(&mut self, checkpoint_id: CheckpointId, values: Vec<String>);
    fn snapshot(&mut self) -> std::io::Result<()>;
    fn load_latest(&self, checkpoint_id: CheckpointId)
        -> std::io::Result<HashMap<u16, StateValue>>;
}
