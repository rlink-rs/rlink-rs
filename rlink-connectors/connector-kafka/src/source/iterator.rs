use std::borrow::BorrowMut;

use rlink::channel::utils::handover::Handover;
use rlink::core::element::Record;

use crate::source::checkpoint::KafkaSourceStateRecorder;
use crate::source::{is_empty_record, ConsumerRecord};

/// Simulate a Kafka consumption stream as an iterator.
/// only support one topic and one partition
pub struct KafkaRecordIterator {
    handover: Handover<ConsumerRecord>,
    state_recorder: KafkaSourceStateRecorder,
}

impl KafkaRecordIterator {
    pub(crate) fn new(
        handover: Handover<ConsumerRecord>,
        state_recorder: KafkaSourceStateRecorder,
    ) -> Self {
        KafkaRecordIterator {
            handover,
            state_recorder,
        }
    }
}

impl Iterator for KafkaRecordIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        match self.handover.poll_next() {
            Ok(mut consumer_record) => {
                if is_empty_record(consumer_record.record.borrow_mut()) {
                    return None;
                }

                self.state_recorder.update(consumer_record.offset);

                Some(consumer_record.record)
            }
            Err(_e) => {
                panic!("kafka input recv channel disconnected");
            }
        }
    }
}
