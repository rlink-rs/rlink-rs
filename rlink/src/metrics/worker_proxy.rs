use tokio::task::JoinHandle;

use crate::utils::http;

pub(crate) async fn collect_worker_metrics(proxy_address: Vec<String>) -> String {
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
