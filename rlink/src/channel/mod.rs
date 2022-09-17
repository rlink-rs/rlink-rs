use std::convert::TryFrom;

use crate::channel::receiver::ChannelReceiver;
use crate::channel::sender::ChannelSender;
use crate::core::element::Element;
use crate::metrics::{register_counter, register_gauge, Tag};

pub const CHANNEL_CAPACITY_PREFIX: &str = "Channel.Capacity.";
pub const CHANNEL_SIZE_PREFIX: &str = "Channel.Size.";
pub const CHANNEL_ACCEPTED_PREFIX: &str = "Channel.Accepted.";
pub const CHANNEL_DRAIN_PREFIX: &str = "Channel.Drain.";

pub type TrySendError<T> = tokio::sync::mpsc::error::TrySendError<T>;
pub type TryRecvError = tokio::sync::mpsc::error::TryRecvError;
pub type SendTimeoutError<T> = tokio::sync::mpsc::error::SendTimeoutError<T>;
pub type SendError<T> = tokio::sync::mpsc::error::SendError<T>;

pub type ElementReceiver = ChannelReceiver<Element>;
pub type ElementSender = ChannelSender<Element>;

pub type Receiver<T> = tokio::sync::mpsc::Receiver<T>;
pub type Sender<T> = tokio::sync::mpsc::Sender<T>;

pub mod receiver;
pub mod sender;
pub mod utils;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ChannelBaseOn {
    Unbounded,
    Bounded,
}

impl<'a> TryFrom<&'a str> for ChannelBaseOn {
    type Error = anyhow::Error;

    fn try_from(mode_str: &'a str) -> Result<Self, Self::Error> {
        let mode_str = mode_str.to_lowercase();
        match mode_str.as_str() {
            "bounded" => Ok(Self::Bounded),
            "unbounded" => Ok(Self::Unbounded),
            _ => Err(anyhow!("Unsupported mode {}", mode_str)),
        }
    }
}

impl std::fmt::Display for ChannelBaseOn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelBaseOn::Bounded => write!(f, "Bounded"),
            ChannelBaseOn::Unbounded => write!(f, "Unbounded"),
        }
    }
}

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
    named_channel_with_base(name, tags, cap, ChannelBaseOn::Bounded)
}

pub fn named_channel_with_base<T>(
    name: &str,
    tags: Vec<Tag>,
    cap: usize,
    base_on: ChannelBaseOn,
) -> (ChannelSender<T>, ChannelReceiver<T>)
where
    T: Sync + Send,
{
    info!(
        "Create channel named with {}, capacity: {}, base on: {}",
        name, cap, base_on
    );

    let (sender, receiver) = match base_on {
        ChannelBaseOn::Bounded => bounded(cap),
        ChannelBaseOn::Unbounded => unimplemented!(), // unbounded(),
    };

    // add_channel_metric(name.to_string(), size.clone(), capacity.clone());
    let size = register_gauge(CHANNEL_SIZE_PREFIX.to_owned() + name, tags.clone());
    let accepted_counter =
        register_counter(CHANNEL_ACCEPTED_PREFIX.to_owned() + name, tags.clone());
    let drain_counter = register_counter(CHANNEL_DRAIN_PREFIX.to_owned() + name, tags);

    (
        ChannelSender::new(name, sender, base_on, cap, size.clone(), accepted_counter),
        ChannelReceiver::new(name, receiver, size.clone(), drain_counter),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::channel::ChannelBaseOn;
    use crate::channel::{bounded, named_channel_with_base};
    use crate::utils::date_time::current_timestamp;

    #[tokio::test]
    pub async fn bounded_test() {
        // let (sender, receiver) = crate::channel::unbounded();
        let (sender, mut receiver) = bounded(10000 * 100);

        tokio::time::sleep(Duration::from_secs(2)).await;

        for _n in 0..100 {
            let sender = sender.clone();
            tokio::spawn(async move {
                for i in 0..10000 {
                    sender.send(i.to_string()).await.unwrap();
                }
            });
        }
        {
            let _a = sender;
        }

        let mut begin = Duration::default();
        while let Some(_n) = receiver.recv().await {
            if begin.as_nanos() == 0 {
                begin = current_timestamp();
            }
        }
        let end = current_timestamp();

        println!("{}", end.checked_sub(begin).unwrap().as_nanos());
    }

    #[tokio::test]
    pub async fn channel_sender_test() {
        let cap = 1 * 1;
        let (sender, mut receiver) =
            named_channel_with_base("", vec![], cap, ChannelBaseOn::Bounded);

        let recv_thread_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let begin = current_timestamp();
            while let Some(_n) = receiver.recv().await {}
            let end = current_timestamp();

            println!("{}", end.checked_sub(begin).unwrap().as_nanos());
        });

        let mut bs = Vec::with_capacity(1024 * 8);
        for _n in 0..bs.capacity() {
            bs.push('a' as u8);
        }
        let s = String::from_utf8(bs).unwrap();

        let send_thread_handle = tokio::spawn(async move {
            for _n in 0..100 * 10000 {
                sender.send(s.clone()).await.unwrap();
            }

            tokio::time::sleep(Duration::from_secs(10)).await;
            for _n in 0..cap {
                sender.send("".to_string()).await.unwrap();
            }
        });

        send_thread_handle.await.unwrap();
        recv_thread_handle.await.unwrap();
        println!("finish");
    }
}
