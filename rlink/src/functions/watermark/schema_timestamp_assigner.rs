use crate::core::data_types::Schema;
use crate::core::element::Record;
use crate::core::function::Context;
use crate::core::watermark::TimestampAssigner;

#[derive(Debug)]
pub struct SchemaTimestampAssigner {
    field_types: Schema,
    column: usize,
}

impl SchemaTimestampAssigner {
    pub fn new(column: usize) -> Self {
        SchemaTimestampAssigner {
            column,
            field_types: Schema::empty(),
        }
    }
}

impl TimestampAssigner for SchemaTimestampAssigner {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        self.field_types = context.input_schema.first().clone();
        Ok(())
    }

    fn extract_timestamp(&mut self, row: &mut Record, _previous_element_timestamp: u64) -> u64 {
        let reader = row.as_reader(self.field_types.as_type_ids());
        reader.get_u64(self.column).unwrap()
    }
}
