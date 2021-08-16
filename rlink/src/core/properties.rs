use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::Index;
use std::str::FromStr;
use std::time::Duration;

use crate::core::backend::{CheckpointBackend, KeyedStateBackend};
use crate::core::cluster::MetadataStorageType;

pub type ClusterMode = crate::runtime::ClusterMode;
pub type ChannelBaseOn = crate::channel::ChannelBaseOn;

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

    fn set_pub_sub_channel_base(&mut self, base_on: ChannelBaseOn);
    fn get_pub_sub_channel_base(&self) -> anyhow::Result<ChannelBaseOn>;

    fn set_speed_backpressure_interval(&mut self, interval: Duration);
    fn get_speed_backpressure_interval(&self) -> anyhow::Result<Duration>;

    fn set_speed_backpressure_max_times(&mut self, max_times: usize);
    fn get_speed_backpressure_max_times(&self) -> anyhow::Result<usize>;

    fn set_speed_backpressure_pause_time(&mut self, interval: Duration);
    fn get_speed_backpressure_pause_time(&self) -> anyhow::Result<Duration>;
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

    pub fn get_sub_properties(&self, pre_key: &str) -> Properties {
        let pre_key = format!("{}.", pre_key);
        let mut properties = Properties::new();
        for (key, value) in self.as_map() {
            if key.starts_with(pre_key.as_str()) {
                let key = key.index(pre_key.len()..);
                properties.set_string(key.to_owned(), value.to_owned());
            }
        }
        properties
    }

    pub fn get_source_properties(&self, fn_name: &str) -> Properties {
        let pre_key = format!("source.{fn_name}", fn_name = fn_name);
        self.get_sub_properties(pre_key.as_str())
    }

    pub fn get_window_properties(&self, fn_name: &str) -> Properties {
        let pre_key = format!("window.{fn_name}", fn_name = fn_name);
        self.get_sub_properties(pre_key.as_str())
    }

    pub fn get_reduce_properties(&self, fn_name: &str) -> Properties {
        let pre_key = format!("reduce.{fn_name}", fn_name = fn_name);
        self.get_sub_properties(pre_key.as_str())
    }

    pub fn get_filter_properties(&self, fn_name: &str) -> Properties {
        let pre_key = format!("filter.{fn_name}", fn_name = fn_name);
        self.get_sub_properties(pre_key.as_str())
    }

    pub fn get_sink_properties(&self, fn_name: &str) -> Properties {
        let pre_key = format!("sink.{fn_name}", fn_name = fn_name);
        self.get_sub_properties(pre_key.as_str())
    }

    pub fn get_custom_properties(&self, fn_name: &str) -> Properties {
        let pre_key = format!("custom.{fn_name}", fn_name = fn_name);
        self.get_sub_properties(pre_key.as_str())
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
const SYSTEM_PUB_SUB_CHANNEL_BASE_ON: &str = "SYSTEM_PUB_SUB_CHANNEL_BASE_ON";
const SYSTEM_SPEED_BACKPRESSURE_INTERVAL: &str = "SYSTEM_SPEED_BACKPRESSURE_INTERVAL";
const SYSTEM_SPEED_BACKPRESSURE_MAX_TIMES: &str = "SYSTEM_SPEED_BACKPRESSURE_MAX_TIMES";
const SYSTEM_SPEED_BACKPRESSURE_PAUSE_TIME: &str = "SYSTEM_SPEED_BACKPRESSURE_PAUSE_TIME";

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

    fn set_pub_sub_channel_base(&mut self, base_on: ChannelBaseOn) {
        let value = format!("{}", base_on);
        self.set_string(SYSTEM_PUB_SUB_CHANNEL_BASE_ON.to_string(), value);
    }

    fn get_pub_sub_channel_base(&self) -> anyhow::Result<ChannelBaseOn> {
        let value = self.get_string(SYSTEM_PUB_SUB_CHANNEL_BASE_ON)?;
        ChannelBaseOn::try_from(value.as_str()).map_err(|e| anyhow!(e))
    }

    fn set_speed_backpressure_interval(&mut self, interval: Duration) {
        self.set_duration(SYSTEM_SPEED_BACKPRESSURE_INTERVAL, interval);
    }

    fn get_speed_backpressure_interval(&self) -> anyhow::Result<Duration> {
        self.get_duration(SYSTEM_SPEED_BACKPRESSURE_INTERVAL)
    }

    fn set_speed_backpressure_max_times(&mut self, max_times: usize) {
        self.set_usize(SYSTEM_SPEED_BACKPRESSURE_MAX_TIMES, max_times);
    }

    fn get_speed_backpressure_max_times(&self) -> anyhow::Result<usize> {
        self.get_usize(SYSTEM_SPEED_BACKPRESSURE_MAX_TIMES)
    }

    fn set_speed_backpressure_pause_time(&mut self, interval: Duration) {
        self.set_duration(SYSTEM_SPEED_BACKPRESSURE_PAUSE_TIME, interval);
    }

    fn get_speed_backpressure_pause_time(&self) -> anyhow::Result<Duration> {
        self.get_duration(SYSTEM_SPEED_BACKPRESSURE_PAUSE_TIME)
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
        let sub_properties = properties.get_sub_properties("a.b");
        println!("{:?}", properties);
        println!("{:?}", sub_properties);
    }
}
