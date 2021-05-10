use std::time::Duration;

use crate::channel::{bounded, Receiver, Sender, TryRecvError, TrySendError};
use crate::core::checkpoint::Checkpoint;
use crate::core::cluster::StdResponse;
use crate::utils::date_time;
use crate::utils::http::client::post;
use crate::utils::thread::async_sleep;

pub struct CheckpointChannel {
    sender: Sender<Checkpoint>,
    receiver: Receiver<Checkpoint>,
}

impl CheckpointChannel {
    pub fn new() -> Self {
        let (sender, receiver) = bounded::<Checkpoint>(100);
        CheckpointChannel { sender, receiver }
    }
}

lazy_static! {
    static ref CK_CHANNEL: CheckpointChannel = CheckpointChannel::new();
}

pub(crate) fn submit_checkpoint(ck: Checkpoint) -> Option<Checkpoint> {
    let ck_channel = &*CK_CHANNEL;

    debug!("report checkpoint: {:?}", &ck);
    match ck_channel.sender.try_send(ck) {
        Ok(_) => None,
        Err(TrySendError::Full(ck)) => Some(ck),
        Err(TrySendError::Disconnected(_ck)) => panic!("the Checkpoint channel is disconnected"),
    }
}

pub(crate) async fn start_report_checkpoint(coordinator_address: String) {
    info!("checkpoint loop starting...");

    let ck_channel = &*CK_CHANNEL;

    loop {
        match ck_channel.receiver.try_recv() {
            Ok(ck) => {
                report_checkpoint(coordinator_address.as_str(), ck).await;
            }
            Err(TryRecvError::Empty) => {
                async_sleep(Duration::from_secs(2)).await;
            }
            Err(TryRecvError::Disconnected) => {
                panic!("the Checkpoint channel is disconnected")
            }
        }
    }
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
