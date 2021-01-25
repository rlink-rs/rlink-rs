use std::time::Duration;

use rlink::api::element::Record;
use rlink::api::runtime::JobId;
use rlink::channel::receiver::ChannelReceiver;
use rlink::channel::sender::ChannelSender;
use rlink::channel::{named_bounded, TryRecvError};
use rlink::metrics::Tag;

#[derive(Clone)]
pub struct Handover {
    sender: ChannelSender<Record>,
    receiver: ChannelReceiver<Record>,
}

impl Handover {
    pub fn new(
        name: &str,
        topic: &str,
        job_id: JobId,
        task_number: u16,
        buffer_size: usize,
    ) -> Self {
        let tags = vec![
            Tag("topic".to_string(), topic.to_string()),
            Tag("job_id".to_string(), format!("{}", job_id.0)),
            Tag("task_number".to_string(), format!("{}", task_number)),
        ];
        let (sender, receiver) = named_bounded(name, tags, buffer_size);
        Handover { sender, receiver }
    }

    #[inline]
    pub fn try_poll_next(&self) -> Result<Record, TryRecvError> {
        self.receiver.try_recv()
    }

    #[inline]
    pub fn produce(&self, element: Record) {
        self.sender.try_send_loop(element, Duration::from_secs(1))
    }
}
