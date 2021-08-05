pub mod checkpoint;
pub mod consumer;
pub mod deserializer;
pub mod input_format;
pub mod iterator;

#[inline]
pub(crate) fn empty_record() -> rlink::core::element::Record {
    rlink::core::element::Record::with_capacity(0)
}

#[inline]
pub(crate) fn is_empty_record(record: &mut rlink::core::element::Record) -> bool {
    record.as_buffer().len() == 0
}
