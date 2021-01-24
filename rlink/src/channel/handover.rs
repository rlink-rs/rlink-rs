use std::time::Duration;

use crate::api::element::Record;
use crate::channel::receiver::ChannelReceiver;
use crate::channel::sender::ChannelSender;
use crate::channel::{named_bounded, RecvError, TryRecvError, TrySendError};
use crate::metrics::Tag;

#[derive(Clone, Debug)]
pub struct Handover {
    sender: ChannelSender<Record>,
    receiver: ChannelReceiver<Record>,
}

impl Handover {
    pub fn new(name: &str, tags: Vec<Tag>, buffer_size: usize) -> Self {
        let (sender, receiver) = named_bounded(name, tags, buffer_size);
        Handover { sender, receiver }
    }

    #[inline]
    pub fn try_poll_next(&self) -> Result<Record, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline]
    pub fn poll_next(&self) -> Result<Record, RecvError> {
        self.receiver.recv()
    }

    #[inline]
    pub fn try_produce(&self, record: Record) -> Result<(), TrySendError<Record>> {
        self.sender.try_send(record)
    }

    #[inline]
    pub fn produce_always(&self, record: Record) {
        self.sender.try_send_loop(record, Duration::from_secs(1))
    }
}
