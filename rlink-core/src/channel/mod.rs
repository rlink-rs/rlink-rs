use crate::channel::receiver::ChannelReceiver;
use crate::channel::sender::ChannelSender;
use crate::metrics::{register_counter, register_gauge, Tag};

pub const CHANNEL_CAPACITY_PREFIX: &str = "Channel.Capacity.";
pub const CHANNEL_SIZE_PREFIX: &str = "Channel.Size.";
pub const CHANNEL_ACCEPTED_PREFIX: &str = "Channel.Accepted.";
pub const CHANNEL_DRAIN_PREFIX: &str = "Channel.Drain.";

pub type TrySendError<T> = tokio::sync::mpsc::error::TrySendError<T>;
pub type TryRecvError = tokio::sync::mpsc::error::TryRecvError;
pub type SendTimeoutError<T> = tokio::sync::mpsc::error::SendTimeoutError<T>;
pub type SendError<T> = tokio::sync::mpsc::error::SendError<T>;

pub type Receiver<T> = tokio::sync::mpsc::Receiver<T>;
pub type Sender<T> = tokio::sync::mpsc::Sender<T>;

pub mod receiver;
pub mod sender;

pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    tokio::sync::mpsc::channel(cap)
}

pub fn named_channel<T>(
    name: &str,
    tags: Vec<Tag>,
    cap: usize,
) -> (ChannelSender<T>, ChannelReceiver<T>)
where
    T: Sync + Send,
{
    named_channel_with_base(name, tags, cap)
}

pub fn named_channel_with_base<T>(
    name: &str,
    tags: Vec<Tag>,
    cap: usize,
) -> (ChannelSender<T>, ChannelReceiver<T>)
where
    T: Sync + Send,
{
    let (sender, receiver) = bounded(cap);

    let size = register_gauge(CHANNEL_SIZE_PREFIX.to_owned() + name, tags.clone());
    let accepted_counter =
        register_counter(CHANNEL_ACCEPTED_PREFIX.to_owned() + name, tags.clone());
    let drain_counter = register_counter(CHANNEL_DRAIN_PREFIX.to_owned() + name, tags);

    (
        ChannelSender::new(name, sender, size.clone(), accepted_counter),
        ChannelReceiver::new(name, receiver, size.clone(), drain_counter),
    )
}
