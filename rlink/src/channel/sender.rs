use std::time::Duration;

use tokio::sync::mpsc::Sender;
use tokio::time::sleep;

use crate::channel::{ChannelBaseOn, CHANNEL_SIZE_PREFIX};
use crate::channel::{SendError, TrySendError};
use crate::metrics::metric::{Counter, Gauge};

#[derive(Clone)]
pub struct ChannelSender<T>
where
    T: Sync + Send,
{
    name: String,
    #[allow(dead_code)]
    guava_size_name: String,

    sender: Sender<T>,
    base_on: ChannelBaseOn,
    cap: usize,

    size: Gauge,
    counter: Counter,
}

impl<T> ChannelSender<T>
where
    T: Sync + Send,
{
    pub fn new(
        name: &str,
        sender: Sender<T>,
        base_on: ChannelBaseOn,
        cap: usize,
        size: Gauge,
        counter: Counter,
    ) -> Self {
        ChannelSender {
            name: name.to_string(),
            guava_size_name: CHANNEL_SIZE_PREFIX.to_owned() + name,
            sender,
            base_on,
            cap,
            size,
            counter,
        }
    }

    #[inline]
    fn on_success(&self) {
        self.size.fetch_add(1 as i64);
        self.counter.fetch_add(1 as u64);
    }

    pub async fn send(&self, event: T) -> Result<(), SendError<T>> {
        if self.base_on == ChannelBaseOn::Unbounded {
            if self.size.load() > self.cap as i64 {
                let mut times = 0;
                loop {
                    if times < 100 {
                        sleep(Duration::from_millis(10)).await;
                    } else {
                        sleep(Duration::from_secs(1)).await;

                        if times == 130 {
                            warn!("death loop in {} over {} times", self.name, times,);
                        }
                    }

                    if self.size.load() < self.cap as i64 {
                        break;
                    }

                    times += 1;
                }
            }
        }

        self.sender.send(event).await.map(|r| {
            self.on_success();
            r
        })
    }

    pub fn try_send(&self, event: T) -> Result<(), TrySendError<T>> {
        if self.base_on == ChannelBaseOn::Unbounded {
            if self.size.load() > self.cap as i64 {
                return Err(TrySendError::Full(event));
            }
        }

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
