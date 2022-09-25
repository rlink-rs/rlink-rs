use std::fmt::{Debug, Formatter};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::core::cluster::StdResponse;
use crate::core::runtime::AtomicManagerStatus;
use crate::core::runtime::{HeartBeatStatus, ManagerStatus};
use crate::runtime::{HeartbeatItem, HeartbeatRequest};
use crate::utils::http::client::post;
use crate::utils::{date_time, panic};

#[derive(Clone)]
pub struct HeartbeatPublish {
    coordinator_address: String,
    task_manager_id: String,
    coordinator_status: Arc<AtomicManagerStatus>,
}

impl Debug for HeartbeatPublish {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CheckpointPublish").finish()
    }
}

impl HeartbeatPublish {
    pub async fn new(coordinator_address: String, task_manager_id: String) -> Self {
        Self {
            coordinator_address,
            task_manager_id,
            coordinator_status: Arc::new(AtomicManagerStatus::new(ManagerStatus::Pending)),
        }
    }

    pub async fn start_heartbeat_timer(&self) {
        info!("heartbeat timer starting...");

        let heartbeat_publish = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;

                heartbeat_publish
                    .report_heartbeat(vec![HeartbeatItem::HeartBeatStatus(HeartBeatStatus::Ok)])
                    .await;
            }
        });
    }

    pub async fn report(&self, ck: HeartbeatItem) {
        info!("report heartbeat change item: {:?}", &ck);
        self.report_heartbeat(vec![ck]).await;
    }

    pub(crate) fn get_coordinator_status(&self) -> ManagerStatus {
        self.coordinator_status.load(Ordering::Relaxed)
    }

    fn update_coordinator_status(&self, coordinator_status: ManagerStatus) {
        self.coordinator_status
            .store(coordinator_status, Ordering::Relaxed);
    }

    pub(crate) async fn report_heartbeat(&self, mut change_items: Vec<HeartbeatItem>) {
        let url = format!("{}/api/heartbeat", self.coordinator_address.as_str());

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
            task_manager_id: self.task_manager_id.to_string(),
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

                    self.update_coordinator_status(coordinator_status);
                }
            }
            Err(e) => {
                error!("heartbeat error. {}, elapsed: {}ms", e, elapsed);
            }
        };
    }
}
