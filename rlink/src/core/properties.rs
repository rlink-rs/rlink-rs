use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;
use std::time::Duration;

use crate::core::backend::{CheckpointBackend, KeyedStateBackend};
use crate::core::cluster::MetadataStorageType;

pub type ClusterMode = crate::runtime::ClusterMode;
pub type ChannelBaseOn = crate::channel::ChannelBaseOn;

pub trait SystemProperties {
    fn set_metadata_mode(&mut self, metadata_storage_mode: MetadataStorageType);
    fn get_metadata_mode(&self) -> anyhow::Result<MetadataStorageType>;

    fn set_keyed_state_backend(&mut self, state_backend: KeyedStateBackend);
    fn get_keyed_state_backend(&self) -> anyhow::Result<KeyedStateBackend>;

    fn set_checkpoint_internal(&mut self, internal: Duration);
    fn get_checkpoint_internal(&self) -> anyhow::Result<Duration>;

    fn set_checkpoint(&mut self, mode: CheckpointBackend);
    fn get_checkpoint(&self) -> anyhow::Result<CheckpointBackend>;

    fn set_checkpoint_ttl(&mut self, internal: Duration);
    fn get_checkpoint_ttl(&self) -> anyhow::Result<Duration>;

    fn get_cluster_mode(&self) -> anyhow::Result<ClusterMode>;

    fn set_pub_sub_channel_size(&mut self, channel_size: usize);
    fn get_pub_sub_channel_size(&self) -> anyhow::Result<usize>;

    fn set_pub_sub_channel_base(&mut self, base_on: ChannelBaseOn);
    fn get_pub_sub_channel_base(&self) -> anyhow::Result<ChannelBaseOn>;
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
}

pub(crate) const SYSTEM_METADATA_STORAGE_MODE: &str = "SYSTEM_METADATA_STORAGE_MODE";
pub(crate) const SYSTEM_KEYED_STATE_BACKEND: &str = "SYSTEM_KEYED_STATE_BACKEND";
pub(crate) const SYSTEM_CHECKPOINT: &str = "SYSTEM_CHECKPOINT";
pub(crate) const SYSTEM_CHECKPOINT_INTERNAL: &str = "SYSTEM_CHECKPOINT_INTERNAL";
pub(crate) const SYSTEM_CHECKPOINT_TTL: &str = "SYSTEM_CHECKPOINT_TTL";
pub(crate) const SYSTEM_CLUSTER_MODE: &str = "SYSTEM_CLUSTER_MODE";
pub(crate) const SYSTEM_PUB_SUB_CHANNEL_SIZE: &str = "SYSTEM_PUB_SUB_CHANNEL_SIZE";
pub(crate) const SYSTEM_PUB_SUB_CHANNEL_BASE_ON: &str = "SYSTEM_PUB_SUB_CHANNEL_BASE_ON";

impl SystemProperties for Properties {
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

    fn set_checkpoint_internal(&mut self, internal: Duration) {
        let value = format!("{}", internal.as_secs());
        self.set_string(SYSTEM_CHECKPOINT_INTERNAL.to_string(), value)
    }

    fn get_checkpoint_internal(&self) -> anyhow::Result<Duration> {
        let value = self.get_string(SYSTEM_CHECKPOINT_INTERNAL)?;
        u64::from_str(value.as_str())
            .map(|v| Duration::from_secs(v))
            .map_err(|e| anyhow!(e))
    }

    fn set_checkpoint(&mut self, mode: CheckpointBackend) {
        let value = serde_json::to_string(&mode).unwrap();
        self.set_string(SYSTEM_CHECKPOINT.to_string(), value);
    }

    fn get_checkpoint(&self) -> anyhow::Result<CheckpointBackend> {
        let value = self.get_string(SYSTEM_CHECKPOINT)?;
        serde_json::from_str(value.as_str()).map_err(|e| anyhow!(e))
    }

    fn set_checkpoint_ttl(&mut self, internal: Duration) {
        let value = format!("{}", internal.as_secs());
        self.set_string(SYSTEM_CHECKPOINT_TTL.to_string(), value)
    }

    fn get_checkpoint_ttl(&self) -> anyhow::Result<Duration> {
        let value = self.get_string(SYSTEM_CHECKPOINT_TTL)?;
        u64::from_str(value.as_str())
            .map(|v| Duration::from_secs(v))
            .map_err(|e| anyhow!(e))
    }

    fn get_cluster_mode(&self) -> anyhow::Result<ClusterMode> {
        let value = self.get_string(SYSTEM_CLUSTER_MODE)?;
        ClusterMode::try_from(value.as_str())
    }

    fn set_pub_sub_channel_size(&mut self, channel_size: usize) {
        let value = channel_size.to_string();
        self.set_string(SYSTEM_PUB_SUB_CHANNEL_SIZE.to_string(), value);
    }

    fn get_pub_sub_channel_size(&self) -> anyhow::Result<usize> {
        let value = self.get_string(SYSTEM_PUB_SUB_CHANNEL_SIZE)?;
        usize::from_str(value.as_str()).map_err(|e| anyhow!(e))
    }

    fn set_pub_sub_channel_base(&mut self, base_on: ChannelBaseOn) {
        let value = format!("{}", base_on);
        self.set_string(SYSTEM_PUB_SUB_CHANNEL_BASE_ON.to_string(), value);
    }

    fn get_pub_sub_channel_base(&self) -> anyhow::Result<ChannelBaseOn> {
        let value = self.get_string(SYSTEM_PUB_SUB_CHANNEL_BASE_ON)?;
        ChannelBaseOn::try_from(value.as_str()).map_err(|e| anyhow!(e))
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
}
