use tokio::task::JoinHandle;

use crate::core::cluster::MetadataStorageType;
use crate::storage::metadata::{MetadataStorage, TMetadataStorage};
use crate::utils::http;

#[async_trait]
pub trait ProxyAddressLoader: Sync + Send {
    async fn load(&self) -> Vec<String>;
}

pub(crate) struct MetadataProxyAddressLoader {
    with_proxy: bool,
    metadata_storage_type: MetadataStorageType,
}

impl MetadataProxyAddressLoader {
    pub fn new(with_proxy: bool, metadata_storage_type: MetadataStorageType) -> Self {
        MetadataProxyAddressLoader {
            with_proxy,
            metadata_storage_type,
        }
    }
}

#[async_trait]
impl ProxyAddressLoader for MetadataProxyAddressLoader {
    async fn load(&self) -> Vec<String> {
        if !self.with_proxy {
            return vec![];
        }

        match MetadataStorage::new(&self.metadata_storage_type)
            .load()
            .await
        {
            Ok(cluster_descriptor) => cluster_descriptor
                .worker_managers
                .iter()
                .map(|x| x.web_address.clone())
                .collect(),
            Err(_e) => vec![],
        }
    }
}

pub(crate) async fn collect_worker_metrics(
    with_proxy: bool,
    metadata_mode: MetadataStorageType,
) -> String {
    let proxy_addr_loader = MetadataProxyAddressLoader::new(with_proxy, metadata_mode);
    let addrs = proxy_addr_loader.load().await;
    collect_worker_metrics_with_addrs(addrs).await
}

pub(crate) async fn collect_worker_metrics_with_addrs(proxy_address: Vec<String>) -> String {
    let mut result_handles = Vec::with_capacity(proxy_address.len());
    for addr in proxy_address {
        if addr.is_empty() {
            continue;
        }

        let r: JoinHandle<String> = tokio::spawn(async move {
            match http::client::get(addr.as_str()).await {
                Ok(r) => r,
                Err(e) => {
                    error!("proxy {} metrics error, {}", addr, e);
                    "".to_string()
                }
            }
        });

        result_handles.push(r);
    }

    let mut result_str = String::new();
    for r in result_handles {
        match r.await {
            Ok(metrics_msg) => {
                result_str.push_str(metrics_msg.as_str());
                result_str.push_str("\n\n");
            }
            Err(e) => {
                error!("no metrics message found. {}", e);
            }
        }
    }

    result_str
}
