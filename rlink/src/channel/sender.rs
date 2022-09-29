use metrics::{Counter, Gauge};
use tokio::sync::mpsc::Sender;

use crate::channel::CHANNEL_SIZE_PREFIX;
use crate::channel::{SendError, TrySendError};

#[derive(Clone)]
pub struct ChannelSender<T>
where
    T: Sync + Send,
{
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    guava_size_name: String,

    sender: Sender<T>,

    size: Gauge,
    counter: Counter,
}

impl<T> ChannelSender<T>
where
    T: Sync + Send,
{
    pub fn new(name: &str, sender: Sender<T>, size: Gauge, counter: Counter) -> Self {
        ChannelSender {
            name: name.to_string(),
            guava_size_name: CHANNEL_SIZE_PREFIX.to_owned() + name,
            sender,
            size,
            counter,
        }
    }

    #[inline]
    fn on_success(&self) {
        self.size.increment(1 as f64);
        self.counter.increment(1 as u64);
    }

    pub async fn send(&self, event: T) -> Result<(), SendError<T>> {
        self.sender.send(event).await.map(|r| {
            self.on_success();
            r
        })
    }

    pub fn try_send(&self, event: T) -> Result<(), TrySendError<T>> {
        self.sender.try_send(event).map(|r| {
            self.on_success();
            r
        })
    }

    #[inline]
    pub fn try_send_opt(&self, event: T) -> Option<T> {
        match self.try_send(event) {
            Ok(_) => None,
            Err(TrySendError::Full(t)) => Some(t),
            Err(TrySendError::Closed(t)) => Some(t),
        }
    }
}
