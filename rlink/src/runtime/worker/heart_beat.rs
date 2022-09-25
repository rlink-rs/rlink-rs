use std::fmt::{Debug, Formatter};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::channel::{bounded, Receiver, Sender, TrySendError};
use crate::core::cluster::StdResponse;
use crate::core::runtime::AtomicManagerStatus;
use crate::core::runtime::{HeartBeatStatus, ManagerStatus};
use crate::runtime::{HeartbeatItem, HeartbeatRequest};
use crate::utils::http::client::post;
use crate::utils::{date_time, panic};

#[derive(Clone)]
pub struct HeartbeatPublish {
    sender: Option<Sender<HeartbeatItem>>,
    coordinator_status: Arc<AtomicManagerStatus>,
}

impl Debug for HeartbeatPublish {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CheckpointPublish").finish()
    }
}

impl HeartbeatPublish {
    pub async fn new(coordinator_address: String, task_manager_id: String) -> Self {
        let (sender, receiver) = bounded::<HeartbeatItem>(100);

        let heartbeat_publish = Self {
            sender: Some(sender),
            coordinator_status: Arc::new(AtomicManagerStatus::new(ManagerStatus::Pending)),
        };

        heartbeat_publish
            .start_heartbeat_timer(coordinator_address, task_manager_id, receiver)
            .await;

        heartbeat_publish
    }

    async fn start_heartbeat_timer(
        &self,
        coordinator_address: String,
        task_manager_id: String,
        mut receiver: Receiver<HeartbeatItem>,
    ) {
        info!("heartbeat loop starting...");
        {
            let heartbeat_publish = self.clone();
            let coordinator_address = coordinator_address.clone();
            let task_manager_id = task_manager_id.clone();
            tokio::spawn(async move {
                while let Some(hi) = receiver.recv().await {
                    report_heartbeat(
                        coordinator_address.as_str(),
                        task_manager_id.as_str(),
                        vec![hi],
                        heartbeat_publish.clone(),
                    )
                    .await;
                }
            });
        }

        {
            let heartbeat_publish = self.clone();
            let coordinator_address = coordinator_address.clone();
            let task_manager_id = task_manager_id.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

                    report_heartbeat(
                        coordinator_address.as_str(),
                        task_manager_id.as_str(),
                        vec![HeartbeatItem::HeartBeatStatus(HeartBeatStatus::Ok)],
                        heartbeat_publish.clone(),
                    )
                    .await;
                }
            });
        }
    }

    pub fn report(&self, ck: HeartbeatItem) {
        info!("report heartbeat change item: {:?}", &ck);
        match self.sender.as_ref().unwrap().try_send(ck) {
            Ok(_) => {}
            Err(TrySendError::Full(_ck)) => {
                unreachable!()
            }
            Err(TrySendError::Closed(_ck)) => panic!("the Heartbeat channel is disconnected"),
        }
    }

    pub(crate) fn get_coordinator_status(&self) -> ManagerStatus {
        self.coordinator_status.load(Ordering::Relaxed)
    }

    fn update_coordinator_status(&self, coordinator_status: ManagerStatus) {
        self.coordinator_status
            .store(coordinator_status, Ordering::Relaxed);
    }
}

pub(crate) async fn report_heartbeat(
    coordinator_address: &str,
    task_manager_id: &str,
    mut change_items: Vec<HeartbeatItem>,
    heartbeat_publish: HeartbeatPublish,
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

    debug!("<heartbeat> report {}", body);

    let begin_time = date_time::current_timestamp_millis();
    let resp = post::<StdResponse<ManagerStatus>>(url, body).await;
    let end_time = date_time::current_timestamp_millis();
    let elapsed = end_time - begin_time;

    match resp {
        Ok(resp) => {
            if elapsed > 1000 {
                warn!("heartbeat success. {:?}, elapsed: {}ms > 1s", resp, elapsed);
            }

            if let Some(coordinator_status) = resp.data {
                match coordinator_status {
                    ManagerStatus::Terminating | ManagerStatus::Terminated => {
                        info!("coordinator status: {:?}", coordinator_status)
                    }
                    _ => {}
                }

                heartbeat_publish.update_coordinator_status(coordinator_status);
            }
        }
        Err(e) => {
            error!("heartbeat error. {}, elapsed: {}ms", e, elapsed);
        }
    };
}
