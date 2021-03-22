use std::net::SocketAddr;
use std::time::Duration;

use metrics_util::MetricKindMask;
use rand::prelude::*;

use crate::metrics::prometheus_exporter::PrometheusBuilder;

pub mod global_metrics;
mod prometheus_exporter;
mod worker_proxy;

pub use global_metrics::register_counter;
pub use global_metrics::register_gauge;
pub use global_metrics::Tag;

pub(crate) fn install(addr: SocketAddr, with_proxy: bool) -> anyhow::Result<()> {
    PrometheusBuilder::new()
        .listen_address(addr)
        .idle_timeout(
            MetricKindMask::COUNTER | MetricKindMask::HISTOGRAM,
            Some(Duration::from_secs(10)),
        )
        .install(with_proxy)?;

    Ok(())
}

pub(crate) fn init_metrics(bind_ip: &str, with_proxy: bool) -> Option<SocketAddr> {
    let mut rng = rand::thread_rng();
    let loops = 30;
    for _index in 0..loops {
        let port = rng.gen_range(10000..30000);
        let addr: SocketAddr = format!("{}:{}", bind_ip, port)
            .as_str()
            .parse()
            .expect("failed to parse http listen address");

        match install(addr, with_proxy) {
            Ok(_) => {
                info!(
                    "metrics prometheus http exporter listen on http://{}, proxy mode={}",
                    addr.to_string(),
                    with_proxy
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
    use crate::metrics::install;

    #[test]
    pub fn install_test() {
        install("127.0.0.1:9000".parse().unwrap(), false).unwrap();
        // install("127.0.0.1:9000".parse().unwrap(), false).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(300000));
    }
}
