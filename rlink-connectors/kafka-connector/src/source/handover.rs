use rlink::api::element::Record;
use rlink::channel::receiver::ChannelReceiver;
use rlink::channel::sender::ChannelSender;
use rlink::channel::{named_bounded, TryRecvError, TrySendError};
use rlink::metrics::Tag;

#[derive(Clone)]
pub struct Handover {
    sender: ChannelSender<Record>,
    receiver: ChannelReceiver<Record>,
}

impl Handover {
    pub fn new(topic: &str, partition: i32, buffer_size: usize) -> Self {
        let tags = vec![
            Tag("topic".to_string(), topic.to_string()),
            Tag("partition".to_string(), format!("{}", partition)),
        ];

        let (sender, receiver) = named_bounded("KafkaSource_Handover", tags, buffer_size);
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
