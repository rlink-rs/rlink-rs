use std::net::SocketAddr;
use std::time::Duration;

use metrics_runtime::observers::PrometheusBuilder;
use metrics_runtime::Receiver;
use rand::prelude::*;

use crate::utils::get_runtime;
// use metrics_exporter_http::HttpExporter;
use crate::channel::bounded;
use crate::metrics::exporter_http::HttpExporter;

mod exporter_http;
pub mod global_metrics;
mod worker_proxy;

pub use global_metrics::register_counter;
pub use global_metrics::register_gauge;
pub use global_metrics::Tag;

// pub fn init_metrics() -> SocketAddr {
//     let receiver = Receiver::builder()
//         .histogram(Duration::from_secs(5), Duration::from_millis(100))
//         .build()
//         .expect("failed to build receiver");
//
//     let controller = receiver.controller();
//
//     let addr = "0.0.0.0:9939"
//         .parse()
//         .expect("failed to parse http listen address");
//     //    let builder = JsonBuilder::new().set_pretty_json(true);
//     let builder = PrometheusBuilder::new();
//     let exporter = HttpExporter::new(controller.clone(), builder, addr);
//
//     std::thread::spawn(move || {
//         // exporter.run()
//         get_runtime().block_on(exporter.async_run()).unwrap();
//     });
//
//     receiver.install();
//     info!("receiver configured");
//
//     addr
// }

pub(crate) fn init_metrics2(bind_ip: &str, with_proxy: bool) -> SocketAddr {
    let receiver = Receiver::builder()
        .histogram(Duration::from_secs(5), Duration::from_millis(100))
        .build()
        .expect("failed to build receiver");

    let controller = receiver.controller();

    let (tx, rx) = bounded(1);
    let bind_ip = bind_ip.to_string();
    let tx_clone = tx.clone();
    std::thread::spawn(move || {
        get_runtime().block_on(async move {
            let mut rng = rand::thread_rng();
            let loops = 30;
            for _index in 0..loops {
                let port = rng.gen_range(10000, 30000);
                // todo for test
                // if port > 0 {
                //     port = 9939;
                // };

                let address = format!("{}:{}", bind_ip, port);
                let addr: SocketAddr = address
                    .parse()
                    .expect("failed to parse http listen address");
                //    let builder = JsonBuilder::new().set_pretty_json(true);
                let builder = PrometheusBuilder::new();
                let exporter = HttpExporter::new(controller.clone(), builder);
                match exporter.try_bind(&addr) {
                    Ok(addr_incoming) => {
                        tx_clone.send(addr.clone()).unwrap();
                        exporter
                            .async_run1(addr_incoming, with_proxy)
                            .await
                            .unwrap();
                        break;
                    }
                    Err(e) => warn!("exporter run error. {}", e),
                }
            }
        });
    });

    let addr = rx.recv().unwrap();
    info!(
        "metrics http exporter listen on http://{}, proxy mode={}",
        addr.to_string(),
        with_proxy
    );

    receiver.install();
    info!("receiver configured");

    addr
}

// #[cfg(test)]
// mod tests {
//     use crate::metrics::{init_metrics, init_metrics2};
//
//     #[test]
//     pub fn port_inuse() {
//         init_metrics();
//         std::thread::park();
//     }
//
//     #[test]
//     pub fn port_inuse1() {
//         let addr = init_metrics2("0.0.0.0", false);
//         println!("{}", addr.to_string());
//         std::thread::park();
//     }
// }
