use std::fmt::{Debug, Formatter};

use crate::channel::{bounded, Receiver, Sender, TrySendError};
use crate::core::checkpoint::Checkpoint;
use crate::core::cluster::StdResponse;
use crate::utils::date_time;
use crate::utils::http::client::post;

#[derive(Clone, Default)]
pub struct CheckpointPublish {
    sender: Option<Sender<Checkpoint>>,
}

impl Debug for CheckpointPublish {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CheckpointPublish").finish()
    }
}

impl CheckpointPublish {
    pub async fn new(coordinator_address: String) -> Self {
        let (sender, receiver) = bounded::<Checkpoint>(100);
        Self::start_report_checkpoint(coordinator_address, receiver).await;

        Self {
            sender: Some(sender),
        }
    }

    async fn start_report_checkpoint(
        coordinator_address: String,
        mut receiver: Receiver<Checkpoint>,
    ) {
        info!("checkpoint loop starting...");
        tokio::spawn(async move {
            while let Some(ck) = receiver.recv().await {
                report_checkpoint(coordinator_address.as_str(), ck).await;
            }
            panic!("the Checkpoint channel is disconnected");
        });
    }

    pub(crate) fn report(&self, ck: Checkpoint) -> Option<Checkpoint> {
        debug!("report checkpoint: {:?}", &ck);
        match self.sender.as_ref().unwrap().try_send(ck) {
            Ok(_) => None,
            Err(TrySendError::Full(ck)) => Some(ck),
            Err(TrySendError::Closed(_ck)) => panic!("the Checkpoint channel is disconnected"),
        }
    }
}

async fn report_checkpoint(coordinator_address: &str, ck: Checkpoint) {
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
