use rlink::api::element::Record;
use rlink::channel::receiver::ChannelReceiver;
use rlink::channel::sender::ChannelSender;
use rlink::channel::{mb, named_bounded, TryRecvError, TrySendError};
use rlink::metrics::Tag;

use crate::SOURCE_CHANNEL_SIZE;

#[derive(Clone)]
pub struct Handover {
    sender: ChannelSender<Record>,
    receiver: ChannelReceiver<Record>,
}

impl Handover {
    pub fn new(topic: &str, partition: i32) -> Self {
        let tags = vec![
            Tag("topic".to_string(), topic.to_string()),
            Tag("partition".to_string(), format!("{}", partition)),
        ];

        let (sender, receiver) =
            named_bounded("KafkaSource_Handover", tags, SOURCE_CHANNEL_SIZE, mb(100));
        Handover { sender, receiver }
    }

    #[inline]
    pub fn poll_next(&self) -> Result<Record, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline]
    pub fn produce(&self, record: Record) -> Result<(), TrySendError<Record>> {
        self.sender.try_send(record)
    }
}
