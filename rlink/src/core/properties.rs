use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::Index;
use std::str::FromStr;
use std::time::Duration;

use crate::core::backend::{CheckpointBackend, KeyedStateBackend};
use crate::core::cluster::MetadataStorageType;

pub type ClusterMode = crate::runtime::ClusterMode;

pub const PARALLELISM: &'static str = "parallelism";

pub(crate) trait InnerSystemProperties {
    fn set_cluster_mode(&mut self, cluster_mode: ClusterMode);
}

pub trait SystemProperties {
    fn set_application_name(&mut self, application_name: &str);
    fn get_application_name(&self) -> String;

    fn set_metadata_mode(&mut self, metadata_storage_mode: MetadataStorageType);
    fn get_metadata_mode(&self) -> anyhow::Result<MetadataStorageType>;

    fn set_keyed_state_backend(&mut self, state_backend: KeyedStateBackend);
    fn get_keyed_state_backend(&self) -> anyhow::Result<KeyedStateBackend>;

    fn set_checkpoint_interval(&mut self, interval: Duration);
    fn get_checkpoint_interval(&self) -> anyhow::Result<Duration>;

    fn set_checkpoint(&mut self, mode: CheckpointBackend);
    fn get_checkpoint(&self) -> anyhow::Result<CheckpointBackend>;

    fn set_checkpoint_ttl(&mut self, ttl: Duration);
    fn get_checkpoint_ttl(&self) -> anyhow::Result<Duration>;

    fn get_cluster_mode(&self) -> anyhow::Result<ClusterMode>;

    fn set_pub_sub_channel_size(&mut self, channel_size: usize);
    fn get_pub_sub_channel_size(&self) -> anyhow::Result<usize>;
}

pub trait FunctionProperties {
    fn to_source(&self, fn_name: &str) -> Properties;
    fn extend_source(&mut self, fn_name: &str, properties: Properties);

    fn to_window(&self, fn_name: &str) -> Properties;
    fn extend_window(&mut self, fn_name: &str, properties: Properties);

    fn to_reduce(&self, fn_name: &str) -> Properties;
    fn extend_reduce(&mut self, fn_name: &str, properties: Properties);

    fn to_filter(&self, fn_name: &str) -> Properties;
    fn extend_filter(&mut self, fn_name: &str, properties: Properties);

    fn to_sink(&self, fn_name: &str) -> Properties;
    fn extend_sink(&mut self, fn_name: &str, properties: Properties);

    fn to_custom(&self, fn_name: &str) -> Properties;
    fn extend_custom(&mut self, fn_name: &str, properties: Properties);
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Properties {
    name: String,
    properties: HashMap<String, String>,
}

impl Properties {
    pub fn new() -> Self {
        Properties {
            name: "".to_string(),
            properties: HashMap::new(),
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn as_map(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn is_empty(&self) -> bool {
        self.properties.is_empty()
    }

    pub fn set_str(&mut self, key: &str, value: &str) {
        self.properties.insert(key.to_string(), value.to_string());
    }

    pub fn set_string(&mut self, key: String, value: String) {
        self.properties.insert(key, value);
    }

    pub fn get_string(&self, key: &str) -> anyhow::Result<String> {
        match self.properties.get(key) {
            Some(v) => Ok(v.clone()),
            None => Err(anyhow!("`{}` field not found", key)),
        }
    }

    pub fn set_i16(&mut self, key: &str, value: i16) {
        self.set_string(key.to_string(), value.to_string());
    }

    pub fn get_i16(&self, key: &str) -> anyhow::Result<i16> {
        match self.properties.get(key) {
            Some(v) => i16::from_str(v).map_err(|e| anyhow!(e)),
            None => Err(anyhow!("`{}` field not found", key)),
        }
    }

    pub fn set_u16(&mut self, key: &str, value: u16) {
        self.set_string(key.to_string(), value.to_string());
    }

    pub fn get_u16(&self, key: &str) -> anyhow::Result<u16> {
        match self.properties.get(key) {
            Some(v) => u16::from_str(v).map_err(|e| anyhow!(e)),
            None => Err(anyhow!("`{}` field not found", key)),
        }
    }

    pub fn set_i32(&mut self, key: &str, value: i32) {
        self.set_string(key.to_string(), value.to_string());
    }

    pub fn get_i32(&self, key: &str) -> anyhow::Result<i32> {
        match self.properties.get(key) {
            Some(v) => i32::from_str(v).map_err(|e| anyhow!(e)),
            None => Err(anyhow!("`{}` field not found", key)),
        }
    }

    pub fn set_u32(&mut self, key: &str, value: u32) {
        self.set_string(key.to_string(), value.to_string());
    }

    pub fn get_u32(&self, key: &str) -> anyhow::Result<u32> {
        match self.properties.get(key) {
            Some(v) => u32::from_str(v).map_err(|e| anyhow!(e)),
            None => Err(anyhow!("`{}` field not found", key)),
        }
    }

    pub fn set_usize(&mut self, key: &str, value: usize) {
        self.set_u32(key, value as u32)
    }

    pub fn get_usize(&self, key: &str) -> anyhow::Result<usize> {
        self.get_u32(key).map(|x| x as usize)
    }

    pub fn set_i64(&mut self, key: &str, value: i64) {
        self.set_string(key.to_string(), value.to_string());
    }

    pub fn get_i64(&self, key: &str) -> anyhow::Result<i64> {
        match self.properties.get(key) {
            Some(v) => i64::from_str(v).map_err(|e| anyhow!(e)),
            None => Err(anyhow!("`{}` field not found", key)),
        }
    }

    pub fn set_u64(&mut self, key: &str, value: u64) {
        self.set_string(key.to_string(), value.to_string());
    }

    pub fn get_u64(&self, key: &str) -> anyhow::Result<u64> {
        match self.properties.get(key) {
            Some(v) => u64::from_str(v).map_err(|e| anyhow!(e)),
            None => Err(anyhow!("`{}` field not found", key)),
        }
    }

    pub fn set_bool(&mut self, key: &str, value: bool) {
        self.set_string(key.to_string(), value.to_string());
    }

    pub fn get_bool(&self, key: &str) -> anyhow::Result<bool> {
        match self.properties.get(key) {
            Some(v) => bool::from_str(v).map_err(|e| anyhow!(e)),
            None => Err(anyhow!("`{}` field not found", key)),
        }
    }

    pub fn set_duration(&mut self, key: &str, interval: Duration) {
        self.set_u64(key, interval.as_millis() as u64);
    }

    pub fn get_duration(&self, key: &str) -> anyhow::Result<Duration> {
        let value = self.get_u64(key)?;
        Ok(Duration::from_millis(value))
    }

    pub fn to_sub_properties(&self, prefix_key: &str) -> Properties {
        self.to_sub_properties_with_name(prefix_key, None)
    }

    pub fn to_sub_properties_with_name(&self, prefix_key: &str, name: Option<&str>) -> Properties {
        let mut properties = Properties::new();

        if let Some(name) = name {
            properties.name = name.to_string();
        }

        let pre_key = format!("{}.", prefix_key);
        for (key, value) in self.as_map() {
            if key.starts_with(pre_key.as_str()) {
                let key = key.index(pre_key.len()..);
                properties.set_string(key.to_owned(), value.to_owned());
            }
        }

        properties
    }

    pub fn extend_sub_properties(&mut self, prefix_key: &str, properties: Properties) {
        for (sub_key, sub_val) in properties.as_map() {
            let key = format!("{}.{}", prefix_key, sub_key);
            self.properties.insert(key, sub_val.to_string());
        }
    }

    pub fn to_lines_string(&self) -> String {
        let mut lines: Vec<String> = self
            .properties
            .iter()
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect();
        lines.sort();
        lines.join("\n")
    }
}

impl FunctionProperties for Properties {
    fn to_source(&self, fn_name: &str) -> Properties {
        let prefix_key = format!("source.{fn_name}", fn_name = fn_name);
        self.to_sub_properties_with_name(prefix_key.as_str(), Some(fn_name))
    }

    fn extend_source(&mut self, fn_name: &str, properties: Properties) {
        let prefix_key = format!("source.{fn_name}", fn_name = fn_name);
        self.extend_sub_properties(prefix_key.as_str(), properties);
    }

    fn to_window(&self, fn_name: &str) -> Properties {
        let prefix_key = format!("window.{fn_name}", fn_name = fn_name);
        self.to_sub_properties_with_name(prefix_key.as_str(), Some(fn_name))
    }

    fn extend_window(&mut self, fn_name: &str, properties: Properties) {
        let prefix_key = format!("window.{fn_name}", fn_name = fn_name);
        self.extend_sub_properties(prefix_key.as_str(), properties);
    }

    fn to_reduce(&self, fn_name: &str) -> Properties {
        let prefix_key = format!("reduce.{fn_name}", fn_name = fn_name);
        self.to_sub_properties_with_name(prefix_key.as_str(), Some(fn_name))
    }

    fn extend_reduce(&mut self, fn_name: &str, properties: Properties) {
        let prefix_key = format!("reduce.{fn_name}", fn_name = fn_name);
        self.extend_sub_properties(prefix_key.as_str(), properties);
    }

    fn to_filter(&self, fn_name: &str) -> Properties {
        let pre_key = format!("filter.{fn_name}", fn_name = fn_name);
        self.to_sub_properties_with_name(pre_key.as_str(), Some(fn_name))
    }

    fn extend_filter(&mut self, fn_name: &str, properties: Properties) {
        let prefix_key = format!("filter.{fn_name}", fn_name = fn_name);
        self.extend_sub_properties(prefix_key.as_str(), properties);
    }

    fn to_sink(&self, fn_name: &str) -> Properties {
        let prefix_key = format!("sink.{fn_name}", fn_name = fn_name);
        self.to_sub_properties_with_name(prefix_key.as_str(), Some(fn_name))
    }

    fn extend_sink(&mut self, fn_name: &str, properties: Properties) {
        let prefix_key = format!("sink.{fn_name}", fn_name = fn_name);
        self.extend_sub_properties(prefix_key.as_str(), properties);
    }

    fn to_custom(&self, fn_name: &str) -> Properties {
        let prefix_key = format!("custom.{fn_name}", fn_name = fn_name);
        self.to_sub_properties_with_name(prefix_key.as_str(), Some(fn_name))
    }

    fn extend_custom(&mut self, fn_name: &str, properties: Properties) {
        let prefix_key = format!("custom.{fn_name}", fn_name = fn_name);
        self.extend_sub_properties(prefix_key.as_str(), properties);
    }
}

const SYSTEM_APPLICATION_NAME: &str = "SYSTEM_APPLICATION_NAME";
const SYSTEM_METADATA_STORAGE_MODE: &str = "SYSTEM_METADATA_STORAGE_MODE";
const SYSTEM_KEYED_STATE_BACKEND: &str = "SYSTEM_KEYED_STATE_BACKEND";
const SYSTEM_CHECKPOINT: &str = "SYSTEM_CHECKPOINT";
const SYSTEM_CHECKPOINT_INTERVAL: &str = "SYSTEM_CHECKPOINT_INTERVAL";
const SYSTEM_CHECKPOINT_TTL: &str = "SYSTEM_CHECKPOINT_TTL";
const SYSTEM_CLUSTER_MODE: &str = "SYSTEM_CLUSTER_MODE";
const SYSTEM_PUB_SUB_CHANNEL_SIZE: &str = "SYSTEM_PUB_SUB_CHANNEL_SIZE";

impl SystemProperties for Properties {
    fn set_application_name(&mut self, application_name: &str) {
        if let Ok(_v) = self.get_string(SYSTEM_APPLICATION_NAME) {
            panic!("the `application_name` field always exist")
        }

        self.set_string(
            SYSTEM_APPLICATION_NAME.to_string(),
            application_name.to_string(),
        );
    }

    fn get_application_name(&self) -> String {
        self.get_string(SYSTEM_APPLICATION_NAME)
            .expect("Properties must contain `application_name` field")
    }

    fn set_metadata_mode(&mut self, metadata_storage_mode: MetadataStorageType) {
        let value = serde_json::to_string(&metadata_storage_mode).unwrap();
        self.set_string(SYSTEM_METADATA_STORAGE_MODE.to_string(), value)
    }

    fn get_metadata_mode(&self) -> anyhow::Result<MetadataStorageType> {
        let value = self.get_string(SYSTEM_METADATA_STORAGE_MODE)?;
        serde_json::from_str(value.as_str()).map_err(|e| anyhow!(e))
    }

    fn set_keyed_state_backend(&mut self, state_backend: KeyedStateBackend) {
        let value = serde_json::to_string(&state_backend).unwrap();
        self.set_string(SYSTEM_KEYED_STATE_BACKEND.to_string(), value)
    }

    fn get_keyed_state_backend(&self) -> anyhow::Result<KeyedStateBackend> {
        let value = self.get_string(SYSTEM_KEYED_STATE_BACKEND)?;
        serde_json::from_str(value.as_str()).map_err(|e| anyhow!(e))
    }

    fn set_checkpoint_interval(&mut self, interval: Duration) {
        self.set_duration(SYSTEM_CHECKPOINT_INTERVAL, interval);
    }

    fn get_checkpoint_interval(&self) -> anyhow::Result<Duration> {
        self.get_duration(SYSTEM_CHECKPOINT_INTERVAL)
    }

    fn set_checkpoint(&mut self, mode: CheckpointBackend) {
        let value = serde_json::to_string(&mode).unwrap();
        self.set_string(SYSTEM_CHECKPOINT.to_string(), value);
    }

    fn get_checkpoint(&self) -> anyhow::Result<CheckpointBackend> {
        let value = self.get_string(SYSTEM_CHECKPOINT)?;
        serde_json::from_str(value.as_str()).map_err(|e| anyhow!(e))
    }

    fn set_checkpoint_ttl(&mut self, ttl: Duration) {
        self.set_duration(SYSTEM_CHECKPOINT_TTL, ttl);
    }

    fn get_checkpoint_ttl(&self) -> anyhow::Result<Duration> {
        self.get_duration(SYSTEM_CHECKPOINT_TTL)
    }

    fn get_cluster_mode(&self) -> anyhow::Result<ClusterMode> {
        let value = self.get_string(SYSTEM_CLUSTER_MODE)?;
        ClusterMode::try_from(value.as_str())
    }

    fn set_pub_sub_channel_size(&mut self, channel_size: usize) {
        self.set_usize(SYSTEM_PUB_SUB_CHANNEL_SIZE, channel_size);
    }

    fn get_pub_sub_channel_size(&self) -> anyhow::Result<usize> {
        self.get_usize(SYSTEM_PUB_SUB_CHANNEL_SIZE)
    }
}

impl InnerSystemProperties for Properties {
    fn set_cluster_mode(&mut self, cluster_mode: ClusterMode) {
        self.set_str(SYSTEM_CLUSTER_MODE, format!("{}", cluster_mode).as_str())
    }
}

#[cfg(test)]
mod tests {
    use crate::core::properties::Properties;

    #[test]
    pub fn row_properties() {
        let properties = Properties::new();
        println!("{:?}", properties.get_string("a"));
        println!("{:?}", properties.get_i32("i32"));
        println!("{:?}", properties.get_u32("u32"));
        println!("{:?}", properties.get_i64("i64"));
        println!("{:?}", properties.get_u64("u64"));
    }

    #[test]
    pub fn test_sub_properties() {
        let mut properties = Properties::new();
        properties.set_str("a.b", "v");
        properties.set_str("a.b.c", "v");
        properties.set_str("a.b.c.d", "v");
        let sub_properties = properties.to_sub_properties("a.b");
        println!("{:?}", properties);
        println!("{:?}", sub_properties);
    }
}
