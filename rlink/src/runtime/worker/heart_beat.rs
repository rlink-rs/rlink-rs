use std::time::Duration;

use crate::channel::{unbounded, Receiver, Sender, TrySendError};
use crate::core::cluster::StdResponse;
use crate::runtime::{HeartBeatStatus, HeartbeatItem, HeartbeatRequest};
use crate::utils::http::client::post;
use crate::utils::thread::async_sleep;
use crate::utils::{date_time, panic};

pub struct HeartbeatChannel {
    sender: Sender<HeartbeatItem>,
    receiver: Receiver<HeartbeatItem>,
}

impl HeartbeatChannel {
    pub fn new() -> Self {
        let (sender, receiver) = unbounded::<HeartbeatItem>();
        HeartbeatChannel { sender, receiver }
    }
}

lazy_static! {
    static ref HB_CHANNEL: HeartbeatChannel = HeartbeatChannel::new();
}

pub(crate) fn submit_heartbeat(ck: HeartbeatItem) {
    let hb_channel = &*HB_CHANNEL;

    debug!("report heartbeat change item: {:?}", &ck);
    match hb_channel.sender.try_send(ck) {
        Ok(_) => {}
        Err(TrySendError::Full(_ck)) => {
            unreachable!()
        }
        Err(TrySendError::Disconnected(_ck)) => panic!("the Heartbeat channel is disconnected"),
    }
}

pub(crate) async fn start_heartbeat_timer(coordinator_address: String, task_manager_id: String) {
    info!("heartbeat loop starting...");
    let hb_channel = &*HB_CHANNEL;

    loop {
        let change_items = {
            let mut change_items = Vec::new();
            while let Ok(ci) = hb_channel.receiver.try_recv() {
                change_items.push(ci);
            }
            change_items
        };

        report_heartbeat(
            coordinator_address.as_str(),
            task_manager_id.as_str(),
            change_items,
        )
        .await;

        async_sleep(Duration::from_secs(10)).await;
    }
}

pub(crate) async fn report_heartbeat(
    coordinator_address: &str,
    task_manager_id: &str,
    mut change_items: Vec<HeartbeatItem>,
) {
    let url = format!("{}/api/heartbeat", coordinator_address);

    let exist_status_item = change_items
        .iter()
        .find(|x| match x {
            HeartbeatItem::HeartBeatStatus(_) => true,
            _ => false,
        })
        .is_some();
    if !exist_status_item {
        let status = {
            if panic::is_panic() {
                HeartBeatStatus::Panic
            } else {
                HeartBeatStatus::Ok
            }
        };
        change_items.push(HeartbeatItem::HeartBeatStatus(status));
    }

    let request = HeartbeatRequest {
        task_manager_id: task_manager_id.to_string(),
        change_items,
    };
    let body = serde_json::to_string(&request).unwrap();

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
