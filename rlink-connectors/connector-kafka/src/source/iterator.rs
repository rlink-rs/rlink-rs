use std::borrow::BorrowMut;

use rlink::channel::utils::handover::Handover;
use rlink::core::element::Record;

use crate::state::KafkaSourceStateRecorder;
use crate::KafkaRecord;

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
                let kafka_record = KafkaRecord::new(record.borrow_mut());

                let topic = kafka_record.get_kafka_topic().unwrap();
                let partition = kafka_record.get_kafka_partition().unwrap();
                let offset = kafka_record.get_kafka_offset().unwrap();

                self.state_recorder.update(topic, partition, offset);

                Some(record)
            }
            Err(_e) => {
                panic!("kafka input recv channel disconnected");
            }
        }
    }
}
