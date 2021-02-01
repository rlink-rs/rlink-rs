use tokio::task::JoinHandle;

use crate::runtime::coordinator::heart_beat::global_application_descriptor;
use crate::runtime::WorkerManagerDescriptor;
use crate::utils::http_client;

pub(crate) async fn collect_worker_metrics() -> String {
    let job_descriptor = global_application_descriptor();
    match job_descriptor {
        Some(job_descriptor) => collect_worker_metrics0(&job_descriptor.worker_managers).await,
        None => "".to_string(),
    }
}

async fn collect_worker_metrics0(workers_address: &Vec<WorkerManagerDescriptor>) -> String {
    let mut result_handles = Vec::new();
    for task_manager_descriptor in workers_address {
        let addr = task_manager_descriptor.metrics_address.clone();
        let r: JoinHandle<String> = tokio::spawn(async move {
            match http_client::get(addr.as_str()).await {
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
