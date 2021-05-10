use rlink::channel::utils::handover::Handover;
use rlink::core::element::Record;

pub struct KafkaRecordIterator {
    handover: Handover,
}

impl KafkaRecordIterator {
    pub fn new(handover: Handover) -> Self {
        KafkaRecordIterator { handover }
    }
}

impl Iterator for KafkaRecordIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        match self.handover.poll_next() {
            Ok(record) => Some(record),
            Err(_e) => {
                panic!("kafka input recv channel disconnected");
            }
        }
    }
}
