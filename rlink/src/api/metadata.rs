use std::fmt::{Display, Formatter};

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(tag = "type", content = "param")]
pub enum MetadataStorageMode {
    Memory,
}

impl MetadataStorageMode {
    pub fn from(
        metadata_storage_mode: &str,
        _metadata_storage_endpoints: &Vec<String>,
        _application_id: &str,
    ) -> Self {
        match metadata_storage_mode.to_ascii_lowercase().as_str() {
            "memory" => MetadataStorageMode::Memory,
            _ => panic!(format!(
                "Not supported `metadata_storage_mode`={}`",
                metadata_storage_mode
            )),
        }
    }
}
impl Display for MetadataStorageMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            MetadataStorageMode::Memory => write!(f, "Memory"),
        }
    }
}
