use std::time::Duration;

use crate::api::cluster::StdResponse;
use crate::runtime::coordinator::server::HeartbeatModel;
use crate::utils::http_client::post;
use crate::utils::thread::get_runtime;
use crate::utils::{date_time, panic};

pub(crate) fn status_heartbeat(
    coordinator_address: &str,
    task_manager_id: &str,
    task_manager_address: &str,
    metrics_address: &str,
) {
    let coordinator_address = coordinator_address.to_string();
    let task_manager_id = task_manager_id.to_string();
    let task_manager_address = task_manager_address.to_string();
    let metrics_address = metrics_address.to_string();

    get_runtime().block_on(status_heartbeat_async(
        coordinator_address.as_str(),
        task_manager_id.as_str(),
        task_manager_address.as_str(),
        metrics_address.as_str(),
    ));
}

pub(crate) async fn status_heartbeat_async(
    coordinator_address: &str,
    task_manager_id: &str,
    task_manager_address: &str,
    metrics_address: &str,
) {
    let url = format!("{}/api/heartbeat", coordinator_address);

    let status = if panic::is_panic() {
        "panic".to_string()
    } else {
        "ok".to_string()
    };
    let model = HeartbeatModel {
        task_manager_id: task_manager_id.to_string(),
        task_manager_address: task_manager_address.to_string(),
        metrics_address: metrics_address.to_string(),
        status,
    };
    let body = serde_json::to_string(&model).unwrap();

    let begin_time = date_time::current_timestamp_millis();
    let resp = post::<StdResponse<bool>>(url, body).await;
    let end_time = date_time::current_timestamp_millis();
    let elapsed = end_time - begin_time;

    match resp {
        Ok(resp) => {
            if elapsed > 1000 {
                warn!("heartbeat success. {:?}, elapsed: {}ms > 1s", resp, elapsed);
            }
        }
        Err(e) => {
            error!("heartbeat error. {}, elapsed: {}ms", e, elapsed);
        }
    };
}

pub(crate) fn start_heart_beat_timer(
    coordinator: &str,
    task_manager_id: &str,
    task_manager_address: &str,
    metrics_address: &str,
) {
    let coordinator = coordinator.to_string();
    let task_manager_id = task_manager_id.to_string();
    let task_manager_address = task_manager_address.to_string();
    let metrics_address = metrics_address.to_string();

    crate::utils::thread::spawn("heartbeat", move || {
        get_runtime().block_on(start_heart_beat_timer_async(
            coordinator.as_str(),
            task_manager_id.as_str(),
            task_manager_address.as_str(),
            metrics_address.as_str(),
        ));
    });
}

pub(crate) async fn start_heart_beat_timer_async(
    coordinator: &str,
    task_manager_id: &str,
    task_manager_address: &str,
    metrics_address: &str,
) {
    info!("heartbeat loop starting...");
    loop {
        tokio::time::delay_for(Duration::from_secs(10)).await;

        status_heartbeat_async(
            coordinator,
            task_manager_id,
            task_manager_address,
            metrics_address,
        )
        .await;
    }
}
