use crate::api::backend::{CheckpointBackend, KeyedStateBackend, OperatorStateBackend};
use crate::api::metadata::MetadataStorageMode;
use std::collections::HashMap;
use std::num::ParseIntError;
use std::str::FromStr;
use std::time::Duration;

pub type ClusterMode = crate::runtime::ClusterMode;

pub trait SystemProperties {
    fn set_metadata_mode(&mut self, metadata_storage_mode: MetadataStorageMode);
    fn get_metadata_mode(&self) -> Result<MetadataStorageMode, PropertiesError>;

    fn set_keyed_state_backend(&mut self, state_backend: KeyedStateBackend);
    fn get_keyed_state_backend(&self) -> Result<KeyedStateBackend, PropertiesError>;

    fn set_operator_state_backend(&mut self, state_backend: OperatorStateBackend);
    fn get_operator_state_backend(&self) -> Result<OperatorStateBackend, PropertiesError>;

    fn set_checkpoint_internal(&mut self, internal: Duration);
    fn get_checkpoint_internal(&self) -> Result<Duration, PropertiesError>;

    fn set_checkpoint(&mut self, mode: CheckpointBackend);
    fn get_checkpoint(&self) -> Result<CheckpointBackend, PropertiesError>;

    fn get_cluster_mode(&self) -> ClusterMode;
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Properties {
    properties: HashMap<String, String>,
}

impl Properties {
    pub fn new() -> Self {
        Properties {
            properties: HashMap::new(),
        }
    }

    pub fn as_map(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn set_str(&mut self, key: &str, value: &str) {
        self.properties.insert(key.to_string(), value.to_string());
    }

    pub fn set_string(&mut self, key: String, value: String) {
        self.properties.insert(key, value);
    }

    pub fn get_string(&self, key: &str) -> Result<String, PropertiesError> {
        match self.properties.get(key) {
            Some(v) => Ok(v.clone()),
            None => Err(PropertiesError::None),
        }
    }

    pub fn set_i32(&mut self, key: &str, value: i32) {
        self.set_string(key.to_string(), value.to_string());
    }

    pub fn get_i32(&self, key: &str) -> Result<i32, PropertiesError> {
        match self.properties.get(key) {
            Some(v) => i32::from_str(v).map_err(|e| PropertiesError::from(e)),
            None => Err(PropertiesError::None),
        }
    }

    pub fn set_u32(&mut self, key: &str, value: u32) {
        self.set_string(key.to_string(), value.to_string());
    }

    pub fn get_u32(&self, key: &str) -> Result<u32, PropertiesError> {
        match self.properties.get(key) {
            Some(v) => u32::from_str(v).map_err(|e| PropertiesError::from(e)),
            None => Err(PropertiesError::None),
        }
    }

    pub fn get_i64(&self, key: &str) -> Result<i64, PropertiesError> {
        match self.properties.get(key) {
            Some(v) => i64::from_str(v).map_err(|e| PropertiesError::from(e)),
            None => Err(PropertiesError::None),
        }
    }

    pub fn get_u64(&self, key: &str) -> Result<u64, PropertiesError> {
        match self.properties.get(key) {
            Some(v) => u64::from_str(v).map_err(|e| PropertiesError::from(e)),
            None => Err(PropertiesError::None),
        }
    }
}

pub(crate) const SYSTEM_CLUSTER_MODE: &str = "SYSTEM_CLUSTER_MODE";

impl SystemProperties for Properties {
    fn set_metadata_mode(&mut self, metadata_storage_mode: MetadataStorageMode) {
        let value = serde_json::to_string(&metadata_storage_mode).unwrap();
        self.set_string("SYSTEM_METADATA_STORAGE_MODE".to_string(), value)
    }

    fn get_metadata_mode(&self) -> Result<MetadataStorageMode, PropertiesError> {
        match self.get_string("SYSTEM_METADATA_STORAGE_MODE") {
            Ok(value) => serde_json::from_str(value.as_str()).map_err(|e| PropertiesError::from(e)),
            Err(e) => Err(e),
        }
    }

    fn set_keyed_state_backend(&mut self, state_backend: KeyedStateBackend) {
        let value = serde_json::to_string(&state_backend).unwrap();
        self.set_string("SYSTEM_KEYED_STATE_BACKEND".to_string(), value)
    }

    fn get_keyed_state_backend(&self) -> Result<KeyedStateBackend, PropertiesError> {
        match self.get_string("SYSTEM_KEYED_STATE_BACKEND") {
            Ok(value) => serde_json::from_str(value.as_str()).map_err(|e| PropertiesError::from(e)),
            Err(e) => Err(e),
        }
    }

    fn set_operator_state_backend(&mut self, state_backend: OperatorStateBackend) {
        let value = serde_json::to_string(&state_backend).unwrap();
        self.set_string("SYSTEM_OPERATOR_STATE_BACKEND".to_string(), value)
    }

    fn get_operator_state_backend(&self) -> Result<OperatorStateBackend, PropertiesError> {
        match self.get_string("SYSTEM_OPERATOR_STATE_BACKEND") {
            Ok(value) => serde_json::from_str(value.as_str()).map_err(|e| PropertiesError::from(e)),
            Err(e) => Err(e),
        }
    }

    fn set_checkpoint_internal(&mut self, internal: Duration) {
        let value = format!("{}", internal.as_secs());
        self.set_string("SYSTEM_CHECKPOINT_INTERNAL".to_string(), value)
    }

    fn get_checkpoint_internal(&self) -> Result<Duration, PropertiesError> {
        match self.get_string("SYSTEM_CHECKPOINT_INTERNAL") {
            Ok(value) => u64::from_str(value.as_str())
                .map(|v| Duration::from_secs(v))
                .map_err(|e| PropertiesError::from(e)),
            Err(e) => Err(e),
        }
    }

    fn set_checkpoint(&mut self, mode: CheckpointBackend) {
        let value = serde_json::to_string(&mode).unwrap();
        self.set_string("SYSTEM_CHECKPOINT".to_string(), value);
    }

    fn get_checkpoint(&self) -> Result<CheckpointBackend, PropertiesError> {
        match self.get_string("SYSTEM_CHECKPOINT") {
            Ok(value) => serde_json::from_str(value.as_str()).map_err(|e| PropertiesError::from(e)),
            Err(e) => Err(e),
        }
    }

    fn get_cluster_mode(&self) -> ClusterMode {
        match self.get_string(SYSTEM_CLUSTER_MODE) {
            Ok(value) => ClusterMode::from(value),
            Err(e) => panic!("ClusterMode not found. {}", e),
        }
    }
}

#[derive(Debug)]
pub enum PropertiesError {
    None,
    Io(std::io::Error),
    ParseIntError(ParseIntError),
    JsonParseError(serde_json::Error),
}

impl std::fmt::Display for PropertiesError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            PropertiesError::None => write!(f, "None error"),
            PropertiesError::Io(ref err) => write!(f, "IO error: {}", err),
            PropertiesError::ParseIntError(ref err) => write!(f, "ParseIntError error: {}", err),
            PropertiesError::JsonParseError(ref err) => write!(f, "JsonParseError error: {}", err),
        }
    }
}

impl std::error::Error for PropertiesError {}

impl From<std::io::Error> for PropertiesError {
    fn from(e: std::io::Error) -> Self {
        PropertiesError::Io(e)
    }
}

impl From<ParseIntError> for PropertiesError {
    fn from(e: ParseIntError) -> Self {
        PropertiesError::ParseIntError(e)
    }
}

impl From<serde_json::Error> for PropertiesError {
    fn from(e: serde_json::Error) -> Self {
        PropertiesError::JsonParseError(e)
    }
}

#[cfg(test)]
mod tests {
    use crate::api::properties::Properties;

    #[test]
    pub fn row_properties() {
        let properties = Properties::new();
        println!("{:?}", properties.get_string("a"));
        println!("{:?}", properties.get_i32("i32"));
        println!("{:?}", properties.get_u32("u32"));
        println!("{:?}", properties.get_i64("i64"));
        println!("{:?}", properties.get_u64("u64"));
    }
}
