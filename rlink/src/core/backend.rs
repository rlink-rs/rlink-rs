use std::fmt::{Debug, Display, Formatter};

/// checkpoint backend storage type
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "param")]
pub enum CheckpointBackend {
    /// pure memory storage
    Memory,
    /// storage in mysql
    MySql {
        /// mysql endpoint
        endpoint: String,
        /// storage table's name, if `None` use default table name
        table: Option<String>,
    },
}

impl Display for CheckpointBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckpointBackend::Memory => write!(f, "Memory"),
            CheckpointBackend::MySql { endpoint, table } => {
                write!(f, "MySql{{endpoint={}}}, table={:?}}}", endpoint, table)
            }
        }
    }
}

/// keyed state backend storage type
#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
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
