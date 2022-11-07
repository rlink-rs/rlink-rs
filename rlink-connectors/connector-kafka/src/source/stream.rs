use futures::Stream;
use rlink::channel::receiver::ChannelReceiver;
use std::borrow::BorrowMut;
use std::pin::Pin;
use std::task::{Context, Poll};

use rlink::core::element::Element;
use rlink::core::function::ElementStream;

use crate::source::checkpoint::KafkaSourceStateRecorder;
use crate::source::{is_empty_record, ConsumerRecord};

/// Simulate a Kafka consumption stream as an iterator.
/// only support one topic and one partition
pub struct KafkaRecordStream {
    receiver: ChannelReceiver<ConsumerRecord>,
    state_recorder: KafkaSourceStateRecorder,
}

impl KafkaRecordStream {
    pub(crate) fn new(
        receiver: ChannelReceiver<ConsumerRecord>,
        state_recorder: KafkaSourceStateRecorder,
    ) -> Self {
        KafkaRecordStream {
            receiver,
            state_recorder,
        }
    }
}

impl ElementStream for KafkaRecordStream {}

impl Stream for KafkaRecordStream {
    type Item = Element;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.as_mut().receiver.poll_recv(cx) {
            Poll::Ready(t) => match t {
                Some(mut consumer_record) => {
                    if is_empty_record(consumer_record.record.borrow_mut()) {
                        return Poll::Ready(None);
                    }

                    self.state_recorder.update(consumer_record.offset);

                    Poll::Ready(Some(Element::Record(consumer_record.record)))
                }
                None => Poll::Ready(None),
            },
            Poll::Pending => Poll::Pending,
        }
    }
}
