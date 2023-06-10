use rlink_core::cluster::{ResponseCode, StdResponse};
use rlink_core::ClusterDescriptor;

// use crate::dag::metadata::DagMetadata;
use crate::dag::dag_manager::DagMetadata;
use crate::utils::http::client::get;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct MetadataLoader {
    coordinator_address: String,
}

impl MetadataLoader {
    pub fn new(coordinator_address: &str) -> Self {
        MetadataLoader {
            coordinator_address: coordinator_address.to_string(),
        }
    }

    pub async fn get_cluster_descriptor(&self) -> anyhow::Result<ClusterDescriptor> {
        let url = format!("{}/api/cluster_metadata", self.coordinator_address);
        loop {
            match get(url.as_str()).await {
                Ok(resp) => {
                    let resp_model: StdResponse<ClusterDescriptor> =
                        serde_json::from_str(resp.as_str()).unwrap();
                    let StdResponse { code, data } = resp_model;
                    if code != ResponseCode::OK {
                        return Err(anyhow!(
                            "get remote JobDescriptor with error code: {}",
                            resp
                        ));
                    }
                    return data.ok_or(anyhow!("no 'ClusterDescriptor' body"));
                }
                Err(e) => {
                    error!("get metadata(`JobDescriptor`) error. {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        }
    }

    pub async fn get_dag_metadata(&self) -> anyhow::Result<DagMetadata> {
        let url = format!("{}/api/dag_metadata", self.coordinator_address);
        loop {
            match get(url.as_str()).await {
                Ok(resp) => {
                    let resp_model: StdResponse<DagMetadata> =
                        serde_json::from_str(resp.as_str()).unwrap();
                    let StdResponse { code, data } = resp_model;
                    if code != ResponseCode::OK {
                        return Err(anyhow!("get remote DagMetadata with error code: {}", resp));
                    }
                    return data.ok_or(anyhow!("no 'DagMetadata' body"));
                }
                Err(e) => {
                    error!("get metadata(`JobDescriptor`) error. {}", e);
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        }
    }
}
