use std::net::SocketAddr;
use std::time::Duration;

use metrics_util::MetricKindMask;
use rand::prelude::*;

use crate::channel::bounded;
use crate::metrics::prometheus_exporter::PrometheusBuilder;

pub mod global_metrics;
mod prometheus_exporter;
mod worker_proxy;

pub use global_metrics::register_counter;
pub use global_metrics::register_gauge;
pub use global_metrics::Tag;

pub(crate) fn init_metrics2(bind_ip: &str, with_proxy: bool) -> Option<SocketAddr> {
    let (tx, rx) = bounded(1);
    let bind_ip = bind_ip.to_string();
    let tx_clone = tx.clone();

    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let mut socket_addr = None;
                let mut rng = rand::thread_rng();
                let loops = 30;
                for _index in 0..loops {
                    let port = rng.gen_range(10000..30000);
                    let addr: SocketAddr = format!("{}:{}", bind_ip, port)
                        .as_str()
                        .parse()
                        .expect("failed to parse http listen address");

                    let builder = PrometheusBuilder::new();
                    let install_result = builder
                        .listen_address(addr)
                        .idle_timeout(
                            MetricKindMask::COUNTER | MetricKindMask::HISTOGRAM,
                            Some(Duration::from_secs(10)),
                        )
                        .install(with_proxy);

                    match install_result {
                        Ok(_) => {
                            socket_addr = Some(addr);
                            break;
                        }
                        Err(e) => {
                            info!("try install PrometheusBuilder on {} failure: {:?}", addr, e);
                            continue;
                        }
                    }
                }

                tx_clone.send(socket_addr.clone()).unwrap();

                if socket_addr.is_some() {
                    loop {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                    }
                }
            })
    });

    let socket_addr = rx.recv().unwrap();
    if socket_addr.is_some() {
        info!(
            "metrics prometheus http exporter listen on http://{}, proxy mode={}",
            socket_addr.as_ref().unwrap().to_string(),
            with_proxy
        );
    }

    socket_addr
}
