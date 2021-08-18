use std::borrow::BorrowMut;

use rlink::channel::utils::handover::Handover;
use rlink::core::element::Record;

use crate::buffer_gen::kafka_message;
use crate::source::is_empty_record;
use crate::state::KafkaSourceStateRecorder;

/// Simulate a Kafka consumption stream as an iterator.
/// only support one topic and one partition
pub struct KafkaRecordIterator {
    handover: Handover,
    state_recorder: KafkaSourceStateRecorder,
}

impl KafkaRecordIterator {
    pub fn new(handover: Handover, state_recorder: KafkaSourceStateRecorder) -> Self {
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
            Ok(mut record) => {
                if is_empty_record(record.borrow_mut()) {
                    return None;
                }

                let kafka_message::Entity {
                    topic,
                    partition,
                    offset,
                    ..
                } = kafka_message::Entity::parse(record.as_buffer()).unwrap();

                self.state_recorder.update(topic, partition, offset);

                Some(record)
            }
            Err(_e) => {
                panic!("kafka input recv channel disconnected");
            }
        }
    }
}
