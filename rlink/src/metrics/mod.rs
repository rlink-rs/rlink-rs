use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use metrics_util::MetricKindMask;
use rand::prelude::*;

use crate::metrics::prometheus_exporter::PrometheusBuilder;

pub mod metric;
mod prometheus_exporter;
mod worker_proxy;

pub use metric::register_counter;
pub use metric::register_gauge;
pub use metric::Tag;

pub trait ProxyAddressLoader: Sync + Send {
    fn load(&self) -> Vec<String>;
}

pub struct DefaultProxyAddressLoader {
    proxy_address: Vec<String>,
}

impl DefaultProxyAddressLoader {
    pub fn new(proxy_address: Vec<String>) -> Self {
        DefaultProxyAddressLoader { proxy_address }
    }
}

impl ProxyAddressLoader for DefaultProxyAddressLoader {
    fn load(&self) -> Vec<String> {
        self.proxy_address.clone()
    }
}

pub(crate) fn install(
    addr: SocketAddr,
    proxy_address_loader: Arc<Box<dyn ProxyAddressLoader>>,
) -> anyhow::Result<()> {
    PrometheusBuilder::new()
        .listen_address(addr)
        .idle_timeout(
            MetricKindMask::COUNTER | MetricKindMask::HISTOGRAM,
            Some(Duration::from_secs(10)),
        )
        .install(proxy_address_loader)?;

    Ok(())
}

pub(crate) fn init_metrics(
    bind_ip: &str,
    proxy_address_loader: Box<dyn ProxyAddressLoader>,
) -> Option<SocketAddr> {
    let proxy_address_loader = Arc::new(proxy_address_loader);

    let mut rng = thread_rng();
    let loops = 30;
    for _index in 0..loops {
        let port = rng.gen_range(10000..30000);
        let addr_str = format!("{}:{}", bind_ip, port);
        let addr: SocketAddr = addr_str
            .as_str()
            .parse()
            .expect(format!("failed to parse http listen address {}", addr_str).as_str());

        match install(addr, proxy_address_loader.clone()) {
            Ok(_) => {
                info!(
                    "metrics prometheus http exporter listen on http://{}",
                    addr.to_string(),
                );
                return Some(addr);
            }
            Err(e) => {
                info!("try install PrometheusBuilder on {} failure: {:?}", addr, e);
                continue;
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::metrics::{install, DefaultProxyAddressLoader};

    #[test]
    pub fn install_test() {
        install(
            "127.0.0.1:9000".parse().unwrap(),
            Arc::new(Box::new(DefaultProxyAddressLoader::new(vec![
                "http://10.181.152.13:17405".to_string(),
            ]))),
        )
        .unwrap();
        // install("127.0.0.1:9000".parse().unwrap(), false).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(300000));
    }
}
