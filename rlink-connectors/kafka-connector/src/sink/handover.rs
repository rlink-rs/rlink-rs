use std::time::Duration;

use rlink::api::element::Record;
use rlink::channel::receiver::ChannelReceiver;
use rlink::channel::sender::ChannelSender;
use rlink::channel::{mb, named_bounded, TryRecvError};
use rlink::metrics::Tag;

use crate::SINK_CHANNEL_SIZE;

#[derive(Clone)]
pub struct Handover {
    sender: ChannelSender<Record>,
    receiver: ChannelReceiver<Record>,
}

impl Handover {
    pub fn new(name: &str, topic: &str, chain_id: u32, task_number: u16) -> Self {
        let tags = vec![
            Tag("topic".to_string(), topic.to_string()),
            Tag("chain_id".to_string(), format!("{}", chain_id)),
            Tag("task_number".to_string(), format!("{}", task_number)),
        ];
        let (sender, receiver) = named_bounded(name, tags, SINK_CHANNEL_SIZE, mb(100));
        Handover { sender, receiver }
    }

    #[inline]
    pub fn poll_next(&self) -> Result<Record, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline]
    pub fn produce(&self, element: Record) {
        self.sender.try_send_loop(element, Duration::from_secs(1))
    }
}
