use crate::core::element::Record;
use crate::core::watermark::TimestampAssigner;

#[derive(Debug)]
pub struct SchemaTimestampAssigner {
    field_types: Vec<u8>,
    column: usize,
}

impl SchemaTimestampAssigner {
    pub fn new(column: usize, field_types: &[u8]) -> Self {
        SchemaTimestampAssigner {
            column,
            field_types: field_types.to_vec(),
        }
    }
}

impl TimestampAssigner for SchemaTimestampAssigner {
    fn extract_timestamp(&mut self, row: &mut Record, _previous_element_timestamp: u64) -> u64 {
        let reader = row.as_reader(self.field_types.as_slice());
        reader.get_u64(self.column).unwrap()
    }
}
