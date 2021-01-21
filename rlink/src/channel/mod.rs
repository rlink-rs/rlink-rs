use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;

use crate::api::element::Element;
use crate::channel::receiver::ChannelReceiver;
use crate::channel::sender::ChannelSender;
use crate::metrics::global_metrics::Tag;

pub const CHANNEL_CAPACITY_PREFIX: &str = "Channel.Capacity.";
pub const CHANNEL_SIZE_PREFIX: &str = "Channel.Size.";
pub const CHANNEL_ACCEPTED_PREFIX: &str = "Channel.Accepted.";
pub const CHANNEL_DRAIN_PREFIX: &str = "Channel.Drain.";

pub type TrySendError<T> = crossbeam::channel::TrySendError<T>;
pub type TryRecvError = crossbeam::channel::TryRecvError;
pub type RecvTimeoutError = crossbeam::channel::RecvTimeoutError;
pub type SendTimeoutError<T> = crossbeam::channel::SendTimeoutError<T>;
pub type RecvError = crossbeam::channel::RecvError;
pub type SendError<T> = crossbeam::channel::SendError<T>;

pub type Receiver<T> = crossbeam::channel::Receiver<T>;
pub type Sender<T> = crossbeam::channel::Sender<T>;

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    crossbeam::channel::unbounded()
}

pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    crossbeam::channel::bounded(cap)
}

pub mod handover;
pub mod receiver;
pub mod sender;

pub type ElementReceiver = ChannelReceiver<Element>;
pub type ElementSender = ChannelSender<Element>;

pub fn named_bounded<T>(
    name: &str,
    tags: Vec<Tag>,
    buffer_size: usize,
) -> (ChannelSender<T>, ChannelReceiver<T>)
where
    T: Clone,
{
    info!("Create channel named with {}", name);

    let size = Arc::new(AtomicI64::new(0));
    let accepted_counter = Arc::new(AtomicU64::new(0));
    let drain_counter = Arc::new(AtomicU64::new(0));
    let (sender, receiver) = bounded(buffer_size);

    // add_channel_metric(name.to_string(), size.clone(), capacity.clone());
    crate::metrics::global_metrics::register_gauge(
        (CHANNEL_SIZE_PREFIX.to_owned() + name).as_str(),
        tags.clone(),
        size.clone(),
    );
    crate::metrics::global_metrics::register_counter(
        (CHANNEL_ACCEPTED_PREFIX.to_owned() + name).as_str(),
        tags.clone(),
        accepted_counter.clone(),
    );
    crate::metrics::global_metrics::register_counter(
        (CHANNEL_DRAIN_PREFIX.to_owned() + name).as_str(),
        tags,
        drain_counter.clone(),
    );

    (
        ChannelSender::new(name, sender, size.clone(), accepted_counter),
        ChannelReceiver::new(name, receiver, size.clone(), drain_counter),
    )
}
