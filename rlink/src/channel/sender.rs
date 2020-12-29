use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

use crate::channel::{
    SendTimeoutError, Sender, TrySendError, CHANNEL_CAPACITY_PREFIX, CHANNEL_SIZE_PREFIX,
};

#[derive(Clone, Debug)]
pub struct ChannelSender<T>
where
    T: Clone,
{
    name: String,
    guava_capacity_name: String,
    guava_size_name: String,
    max_channel_capacity: i64,
    sender: Sender<T>,
    capacity: Arc<AtomicI64>,
    size: Arc<AtomicI64>,
    counter: Arc<AtomicU64>,
}

impl<T> ChannelSender<T>
where
    T: Clone,
{
    pub fn new(
        name: &str,
        max_capacity: i64,
        sender: Sender<T>,
        capacity: Arc<AtomicI64>,
        size: Arc<AtomicI64>,
        counter: Arc<AtomicU64>,
    ) -> Self {
        ChannelSender {
            name: name.to_string(),
            guava_capacity_name: CHANNEL_CAPACITY_PREFIX.to_owned() + name,
            guava_size_name: CHANNEL_SIZE_PREFIX.to_owned() + name,
            max_channel_capacity: max_capacity,
            sender,
            capacity,
            size,
            counter,
        }
    }

    #[inline]
    fn on_success(&self, event_size: i64) {
        self.capacity.fetch_add(event_size, Ordering::Relaxed);
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
        if self.capacity.load(Ordering::Relaxed) as i64 > self.max_channel_capacity {
            std::thread::sleep(timeout);
            Err(SendTimeoutError::Timeout(event))
        } else {
            let size = std::mem::size_of_val::<T>(&event);
            self.sender.send_timeout(event, timeout).map(|r| {
                self.on_success(size as i64);
                r
            })
        }
    }

    pub fn send_timeout_loop(&self, event: T, timeout: Duration) {
        let mut event = event;
        let mut times = 0;
        loop {
            event = match self.send_timeout(event, timeout) {
                Ok(()) => return,
                Err(SendTimeoutError::Timeout(event_back)) => event_back,
                Err(SendTimeoutError::Disconnected(_event_back)) => {
                    panic!("channel Disconnected, {}", self.name)
                }
            };
            times += 1;
            if times % 100 == 0 {
                warn!("death loop in {} over {} times", self.name, times);
            }
        }
    }

    pub fn try_send(&self, event: T) -> Result<(), TrySendError<T>> {
        if self.capacity.load(Ordering::Relaxed) as i64 > self.max_channel_capacity {
            Err(TrySendError::Full(event))
        } else {
            let size = std::mem::size_of_val::<T>(&event);
            self.sender.try_send(event).map(|r| {
                self.on_success(size as i64);
                r
            })
        }
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
