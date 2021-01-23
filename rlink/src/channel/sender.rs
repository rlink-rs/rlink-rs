use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

use crate::channel::{SendError, SendTimeoutError, Sender, TrySendError, CHANNEL_SIZE_PREFIX};

#[derive(Clone, Debug)]
pub struct ChannelSender<T>
where
    T: Clone,
{
    name: String,
    guava_size_name: String,

    sender: Sender<T>,

    size: Arc<AtomicI64>,
    counter: Arc<AtomicU64>,
}

impl<T> ChannelSender<T>
where
    T: Clone,
{
    pub fn new(
        name: &str,
        sender: Sender<T>,
        size: Arc<AtomicI64>,
        counter: Arc<AtomicU64>,
    ) -> Self {
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
        self.size.fetch_add(1 as i64, Ordering::Relaxed);
        self.counter.fetch_add(1 as u64, Ordering::Relaxed);

        // gauge!(
        //     self.guava_capacity_name.clone(),
        //     self.capacity.load(Ordering::Relaxed) as i64
        // );
        // gauge!(
        //     self.guava_size_name.clone(),
        //     self.size.load(Ordering::Relaxed) as i64
        // );
    }

    pub fn send_timeout(&self, event: T, timeout: Duration) -> Result<(), SendTimeoutError<T>> {
        self.sender.send_timeout(event, timeout).map(|r| {
            self.on_success();
            r
        })
    }

    pub fn send(&self, event: T) -> Result<(), SendError<T>> {
        self.sender.send(event).map(|r| {
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

    pub fn try_send_loop(&self, event: T, mut timeout: Duration) {
        let mut event = event;
        let mut times = 0;
        loop {
            event = match self.try_send(event) {
                Ok(()) => return,
                Err(TrySendError::Full(event_back)) => event_back,
                Err(TrySendError::Disconnected(_event_back)) => {
                    panic!("channel Disconnected, {}", self.name)
                }
            };

            std::thread::sleep(timeout);
            times += 1;
            if times % 100 == 0 {
                if times <= 300 {
                    timeout = timeout + timeout;
                }
                warn!(
                    "death loop in {} over {} times, timeout={}s",
                    self.name,
                    times,
                    timeout.as_secs()
                );
            }
        }
    }
}
