use crate::channel::receiver::ChannelReceiver;
use crate::channel::sender::ChannelSender;
use crate::channel::{named_channel, RecvError, SendError, TryRecvError, TrySendError};
use crate::core::element::Record;
use crate::metrics::Tag;
use std::fmt::Debug;

#[derive(Clone, Debug)]
pub struct Handover<T = Record>
where
    T: Debug + Send + Sync,
{
    sender: ChannelSender<T>,
    receiver: ChannelReceiver<T>,
}

impl Handover {
    pub fn new(name: &str, tags: Vec<Tag>, buffer_size: usize) -> Self {
        let (sender, receiver) = named_channel(name, tags, buffer_size);
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
    pub fn produce(&self, record: Record) -> Result<(), SendError<Record>> {
        self.sender.send(record)
    }

    #[inline]
    pub fn try_produce(&self, record: Record) -> Result<(), TrySendError<Record>> {
        self.sender.try_send(record)
    }
}
