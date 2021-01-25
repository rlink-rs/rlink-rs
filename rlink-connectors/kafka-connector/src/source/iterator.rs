use std::borrow::BorrowMut;

use rlink::api::element::Record;
use rlink::channel::handover::Handover;

use crate::source::checkpoint::KafkaCheckpointed;
use crate::KafkaRecord;

pub struct KafkaRecordIterator {
    handover: Handover,
    counter: u64,

    checkpoint: KafkaCheckpointed,
}

impl KafkaRecordIterator {
    pub fn new(handover: Handover, checkpoint: KafkaCheckpointed) -> Self {
        KafkaRecordIterator {
            handover,
            counter: 0,
            checkpoint,
        }
    }
}

impl Iterator for KafkaRecordIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        match self.handover.poll_next() {
            Ok(mut record) => {
                // save to state
                let mut reader = KafkaRecord::new(record.borrow_mut());

                // same as `self.counter % 4096`
                if self.counter & 4095 == 0 {
                    self.checkpoint.get_state().update(
                        reader.get_kafka_topic().unwrap(),
                        reader.get_kafka_partition().unwrap(),
                        reader.get_kafka_offset().unwrap(),
                    );
                }

                self.counter += 1;

                Some(record)
            }
            Err(_e) => {
                panic!("kafka input recv channel disconnected");
            }
        }
    }
}
