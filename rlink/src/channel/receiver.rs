use std::task::{Context, Poll};

use tokio::sync::mpsc::Receiver;

use crate::channel::TryRecvError;
use crate::channel::CHANNEL_SIZE_PREFIX;
use crate::metrics::metric::{Counter, Gauge};

#[derive(Debug)]
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

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        self.receiver.try_recv().map(|event| {
            self.on_success();
            event
        })
    }

    pub async fn recv(&mut self) -> Option<T> {
        let t = self.receiver.recv().await;
        if t.is_some() {
            self.on_success();
        }
        t
    }

    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(t) => {
                if t.is_some() {
                    self.on_success();
                }
                Poll::Ready(t)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    pub fn close(&mut self) {
        self.receiver.close();
    }
}
