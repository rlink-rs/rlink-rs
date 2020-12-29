use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

use crate::channel::{
    Receiver, RecvTimeoutError, TryRecvError, CHANNEL_CAPACITY_PREFIX, CHANNEL_SIZE_PREFIX,
};

#[derive(Clone, Debug)]
pub struct ChannelReceiver<T>
where
    T: Clone,
{
    name: String,
    guava_capacity_name: String,
    guava_size_name: String,
    receiver: Receiver<T>,
    capacity: Arc<AtomicI64>,
    size: Arc<AtomicI64>,
    drain_counter: Arc<AtomicU64>,
}

impl<T> ChannelReceiver<T>
where
    T: Clone,
{
    pub fn new(
        name: &str,
        receiver: Receiver<T>,
        capacity: Arc<AtomicI64>,
        size: Arc<AtomicI64>,
        drain_counter: Arc<AtomicU64>,
    ) -> Self {
        ChannelReceiver {
            name: name.to_string(),
            guava_capacity_name: CHANNEL_CAPACITY_PREFIX.to_owned() + name,
            guava_size_name: CHANNEL_SIZE_PREFIX.to_owned() + name,
            receiver,
            capacity,
            size,
            drain_counter,
        }
    }

    #[inline]
    fn on_success(&self, event: &T) {
        let size = std::mem::size_of_val::<T>(&event);
        self.capacity.fetch_sub(size as i64, Ordering::Relaxed);
        self.size.fetch_sub(1 as i64, Ordering::Relaxed);
        self.drain_counter.fetch_add(1 as u64, Ordering::Relaxed);
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv().map(|event| {
            self.on_success(&event);
            event
        })
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.receiver.recv_timeout(timeout).map(|event| {
            self.on_success(&event);
            event
        })
    }
}
