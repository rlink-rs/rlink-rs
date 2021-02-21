use std::net::SocketAddr;
use std::time::Duration;

use rand::prelude::*;

use crate::channel::bounded;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_util::MetricKindMask;

// mod exporter_http;
pub mod global_metrics;
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
                    let port = rng.gen_range(10000, 30000);
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
                        .install();

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

// pub(crate) fn init_metrics2(bind_ip: &str, with_proxy: bool) -> SocketAddr {
//     let receiver = Receiver::builder()
//         .histogram(Duration::from_secs(5), Duration::from_millis(100))
//         .build()
//         .expect("failed to build receiver");
//
//     let controller = receiver.controller();
//
//     let (tx, rx) = bounded(1);
//     let bind_ip = bind_ip.to_string();
//     let tx_clone = tx.clone();
//     std::thread::spawn(move || {
//         get_runtime().block_on(async move {
//             let mut rng = rand::thread_rng();
//             let loops = 30;
//             for _index in 0..loops {
//                 let port = rng.gen_range(10000, 30000);
//                 // todo for test
//                 // if port > 0 {
//                 //     port = 9939;
//                 // };
//
//                 let address = format!("{}:{}", bind_ip, port);
//                 let addr: SocketAddr = address
//                     .parse()
//                     .expect("failed to parse http listen address");
//                 //    let builder = JsonBuilder::new().set_pretty_json(true);
//                 let builder = PrometheusBuilder::new();
//                 let exporter = HttpExporter::new(controller.clone(), builder);
//                 match exporter.try_bind(&addr) {
//                     Ok(addr_incoming) => {
//                         tx_clone.send(addr.clone()).unwrap();
//                         exporter
//                             .async_run1(addr_incoming, with_proxy)
//                             .await
//                             .unwrap();
//                         break;
//                     }
//                     Err(e) => warn!("exporter run error. {}", e),
//                 }
//             }
//         });
//     });
//
//     let addr = rx.recv().unwrap();
//     info!(
//         "metrics http exporter listen on http://{}, proxy mode={}",
//         addr.to_string(),
//         with_proxy
//     );
//
//     receiver.install();
//     info!("receiver configured");
//
//     addr
// }
