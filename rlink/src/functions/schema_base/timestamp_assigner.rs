use crate::api::checkpoint::CheckpointFunction;
use crate::api::element::Record;
use crate::api::function::NamedFunction;
use crate::api::watermark::TimestampAssigner;

#[derive(Debug)]
pub struct SchemaBaseTimestampAssigner {
    field_types: Vec<u8>,
    column: usize,
}

impl SchemaBaseTimestampAssigner {
    pub fn new(column: usize, field_types: &[u8]) -> Self {
        SchemaBaseTimestampAssigner {
            column,
            field_types: field_types.to_vec(),
        }
    }
}

impl TimestampAssigner for SchemaBaseTimestampAssigner {
    fn extract_timestamp(&mut self, row: &mut Record, _previous_element_timestamp: u64) -> u64 {
        let mut reader = row.as_reader(self.field_types.as_slice());
        reader.get_u64(self.column).unwrap()
    }
}

impl NamedFunction for SchemaBaseTimestampAssigner {
    fn name(&self) -> &str {
        "SchemaBaseTimestampAssigner"
    }
}

impl CheckpointFunction for SchemaBaseTimestampAssigner {}
