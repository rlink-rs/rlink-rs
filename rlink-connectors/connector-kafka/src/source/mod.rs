pub mod builder;
pub mod checkpoint;
pub mod consumer;
pub mod deserializer;
pub mod input_format;
pub mod offset_range;
pub mod stream;

#[inline]
pub(crate) fn empty_record() -> rlink::core::element::Record {
    rlink::core::element::Record::with_capacity(0)
}

#[inline]
pub(crate) fn is_empty_record(record: &mut rlink::core::element::Record) -> bool {
    record.as_buffer().len() == 0
}

#[derive(Clone, Debug)]
pub(crate) struct ConsumerRecord {
    record: rlink::core::element::Record,
    offset: i64,
}

impl ConsumerRecord {
    pub fn new(record: rlink::core::element::Record, offset: i64) -> Self {
        ConsumerRecord { record, offset }
    }
}
