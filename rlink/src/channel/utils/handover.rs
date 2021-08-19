use std::fmt::Debug;

use crate::channel::receiver::ChannelReceiver;
use crate::channel::sender::ChannelSender;
use crate::channel::{named_channel, RecvError, SendError, TryRecvError, TrySendError};
use crate::core::element::Record;
use crate::metrics::Tag;

#[derive(Clone)]
pub struct Handover<T = Record>
where
    T: Debug + Send + Sync,
{
    sender: ChannelSender<T>,
    receiver: ChannelReceiver<T>,
}

impl<T> Handover<T>
where
    T: Debug + Send + Sync,
{
    pub fn new(name: &str, tags: Vec<Tag>, buffer_size: usize) -> Self {
        let (sender, receiver) = named_channel(name, tags, buffer_size);
        Handover { sender, receiver }
    }

    #[inline]
    pub fn try_poll_next(&self) -> Result<T, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline]
    pub fn poll_next(&self) -> Result<T, RecvError> {
        self.receiver.recv()
    }

    #[inline]
    pub fn produce(&self, record: T) -> Result<(), SendError<T>> {
        self.sender.send(record)
    }

    #[inline]
    pub fn try_produce(&self, record: T) -> Result<(), TrySendError<T>> {
        self.sender.try_send(record)
    }
}
