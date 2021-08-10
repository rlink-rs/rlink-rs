use crate::core::element::Record;
use crate::core::function::Context;
use crate::core::watermark::TimestampAssigner;

#[derive(Debug)]
pub struct SchemaTimestampAssigner {
    field_types: Vec<u8>,
    column: usize,
}

impl SchemaTimestampAssigner {
    pub fn new(column: usize) -> Self {
        SchemaTimestampAssigner {
            column,
            field_types: vec![],
        }
    }
}

impl TimestampAssigner for SchemaTimestampAssigner {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        self.field_types = context.input_schema.first().to_vec();
        Ok(())
    }

    fn extract_timestamp(&mut self, row: &mut Record, _previous_element_timestamp: u64) -> u64 {
        let reader = row.as_reader(self.field_types.as_slice());
        reader.get_u64(self.column).unwrap()
    }
}
