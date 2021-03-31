use std::convert::TryFrom;
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
pub type Select<'a> = crossbeam::channel::Select<'a>;

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    crossbeam::channel::unbounded()
}

pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    crossbeam::channel::bounded(cap)
}

pub mod receiver;
pub mod select;
pub mod sender;
pub mod utils;

pub type ElementReceiver = ChannelReceiver<Element>;
pub type ElementSender = ChannelSender<Element>;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BaseOn {
    UnBounded,
    Bounded,
}

impl<'a> TryFrom<&'a str> for BaseOn {
    type Error = anyhow::Error;

    fn try_from(mode_str: &'a str) -> Result<Self, Self::Error> {
        match mode_str {
            "Bounded" => Ok(BaseOn::Bounded),
            "UnBounded" => Ok(BaseOn::UnBounded),
            _ => Err(anyhow!("Unsupported mode {}", mode_str)),
        }
    }
}

impl std::fmt::Display for BaseOn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BaseOn::Bounded => write!(f, "Bounded"),
            BaseOn::UnBounded => write!(f, "UnBounded"),
        }
    }
}

pub fn named_channel<T>(
    name: &str,
    tags: Vec<Tag>,
    cap: usize,
) -> (ChannelSender<T>, ChannelReceiver<T>)
where
    T: Clone,
{
    named_channel_with_base(name, tags, cap, BaseOn::UnBounded)
}

pub fn named_channel_with_base<T>(
    name: &str,
    tags: Vec<Tag>,
    cap: usize,
    base_on: BaseOn,
) -> (ChannelSender<T>, ChannelReceiver<T>)
where
    T: Clone,
{
    info!(
        "Create channel named with {}, capacity: {}, base on: {}",
        name, cap, base_on
    );

    let size = Arc::new(AtomicI64::new(0));
    let accepted_counter = Arc::new(AtomicU64::new(0));
    let drain_counter = Arc::new(AtomicU64::new(0));

    let (sender, receiver) = match base_on {
        BaseOn::Bounded => bounded(cap),
        BaseOn::UnBounded => unbounded(),
    };

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
        ChannelSender::new(name, sender, base_on, cap, size.clone(), accepted_counter),
        ChannelReceiver::new(name, receiver, size.clone(), drain_counter),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::channel::{named_channel_with_base, BaseOn};
    use crate::utils::date_time::current_timestamp;
    use crate::utils::thread::spawn;

    #[test]
    pub fn bounded_test() {
        let (sender, receiver) = crate::channel::unbounded();
        // let (sender, receiver) = crate::channel::bounded(10000 * 100);

        std::thread::sleep(Duration::from_secs(2));

        for n in 0..100 {
            let sender = sender.clone();
            spawn(n.to_string().as_str(), move || {
                for i in 0..10000 {
                    sender.send(i.to_string()).unwrap();
                }
            });
        }
        {
            let _a = sender;
        }

        let mut begin = Duration::default();
        while let Ok(_n) = receiver.recv() {
            if begin.as_nanos() == 0 {
                begin = current_timestamp();
            }
        }
        let end = current_timestamp();

        println!("{}", end.checked_sub(begin).unwrap().as_nanos());
    }

    #[test]
    pub fn channel_sender_test() {
        let cap = 1 * 1;
        let (sender, receiver) = named_channel_with_base("", vec![], cap, BaseOn::Bounded);

        let recv_thread_handle = spawn("recv_thread", move || {
            std::thread::sleep(Duration::from_secs(1));

            let begin = current_timestamp();
            while let Ok(_n) = receiver.recv() {}
            let end = current_timestamp();

            println!("{}", end.checked_sub(begin).unwrap().as_nanos());
        });

        let mut bs = Vec::with_capacity(1024 * 8);
        for _n in 0..bs.capacity() {
            bs.push('a' as u8);
        }
        let s = String::from_utf8(bs).unwrap();

        let send_thread_handle = spawn("send_thread", move || {
            for _n in 0..100 * 10000 {
                sender.send(s.clone()).unwrap();
            }

            std::thread::sleep(Duration::from_secs(10));
            for _n in 0..cap {
                sender.send("".to_string()).unwrap();
            }
        });

        send_thread_handle.join().unwrap();
        recv_thread_handle.join().unwrap();
        println!("finish");
        std::thread::park();
    }
}
