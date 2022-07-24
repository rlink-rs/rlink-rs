use std::time::Duration;

use crate::channel::{Receiver, RecvError, RecvTimeoutError, TryRecvError, CHANNEL_SIZE_PREFIX};
use crate::metrics::metric::{Counter, Gauge};

#[derive(Clone)]
pub struct ChannelReceiver<T>
where
    T: Sync + Send,
{
    #[allow(dead_code)]
    name: String,
    #[allow(dead_code)]
    guava_size_name: String,

    pub(crate) receiver: Receiver<T>,

    size: Gauge,
    drain_counter: Counter,
}

impl<T> ChannelReceiver<T>
where
    T: Sync + Send,
{
    pub fn new(name: &str, receiver: Receiver<T>, size: Gauge, drain_counter: Counter) -> Self {
        ChannelReceiver {
            name: name.to_string(),
            guava_size_name: CHANNEL_SIZE_PREFIX.to_owned() + name,
            receiver,
            size,
            drain_counter,
        }
    }

    #[inline]
    fn on_success(&self) {
        self.size.fetch_sub(1 as i64);
        self.drain_counter.fetch_add(1 as u64);
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv().map(|event| {
            self.on_success();
            event
        })
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        self.receiver.recv().map(|event| {
            self.on_success();
            event
        })
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        self.receiver.recv_timeout(timeout).map(|event| {
            self.on_success();
            event
        })
    }
}
