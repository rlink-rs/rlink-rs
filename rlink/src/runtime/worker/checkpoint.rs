use std::time::Duration;

use crate::channel::{bounded, Sender, TryRecvError, TrySendError};
use crate::core::checkpoint::Checkpoint;
use crate::core::cluster::StdResponse;
use crate::utils::date_time;
use crate::utils::http::client::post;

pub struct CheckpointChannel {
    sender: Option<Sender<Checkpoint>>,
}

static mut CK_CHANNEL: CheckpointChannel = CheckpointChannel { sender: None };

pub(crate) fn submit_checkpoint(ck: Checkpoint) -> Option<Checkpoint> {
    let ck_channel = unsafe { &CK_CHANNEL };

    debug!("report checkpoint: {:?}", &ck);
    match ck_channel.sender.as_ref().unwrap().try_send(ck) {
        Ok(_) => None,
        Err(TrySendError::Full(ck)) => Some(ck),
        Err(TrySendError::Closed(_ck)) => panic!("the Checkpoint channel is disconnected"),
    }
}

pub(crate) async fn start_report_checkpoint(coordinator_address: String) {
    info!("checkpoint loop starting...");

    let (sender, mut receiver) = bounded::<Checkpoint>(100);

    let ck_channel = unsafe { &mut CK_CHANNEL };
    ck_channel.sender = Some(sender);

    tokio::spawn(async move {
        loop {
            match receiver.try_recv() {
                Ok(ck) => {
                    report_checkpoint(coordinator_address.as_str(), ck).await;
                }
                Err(TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Err(TryRecvError::Disconnected) => {
                    panic!("the Checkpoint channel is disconnected")
                }
            }
        }
    });
}

pub(crate) async fn report_checkpoint(coordinator_address: &str, ck: Checkpoint) {
    let url = format!("{}/api/checkpoint", coordinator_address);

    let body = serde_json::to_string(&ck).unwrap();

    let begin_time = date_time::current_timestamp_millis();
    let resp = post::<StdResponse<String>>(url, body).await;
    let end_time = date_time::current_timestamp_millis();
    let elapsed = end_time - begin_time;

    match resp {
        Ok(resp) => {
            if elapsed > 1000 {
                warn!(
                    "report checkpoint success. {:?}, elapsed: {}ms > 1s",
                    resp, elapsed
                );
            }
        }
        Err(e) => {
            error!("report checkpoint error. {}, elapsed: {}ms", e, elapsed);
        }
    };
}
