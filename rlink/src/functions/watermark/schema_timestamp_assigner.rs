use crate::core::data_types::Schema;
use crate::core::element::Record;
use crate::core::function::Context;
use crate::core::watermark::TimestampAssigner;
use crate::functions::column_locate::{ColumnLocate, ColumnLocateBuilder};

#[derive(Debug)]
pub struct SchemaTimestampAssigner {
    schema: Schema,
    column_locate: ColumnLocate,
    column_index: usize,
}

impl SchemaTimestampAssigner {
    pub fn new<T: ColumnLocateBuilder>(column: T) -> Self {
        SchemaTimestampAssigner {
            schema: Schema::empty(),
            column_locate: column.build(),
            column_index: 0,
        }
    }
}

impl TimestampAssigner for SchemaTimestampAssigner {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        self.schema = context.input_schema.first().clone();

        let (index, _field) = self.column_locate.to_column(&self.schema);
        self.column_index = index;

        Ok(())
    }

    fn extract_timestamp(&mut self, row: &mut Record, _previous_element_timestamp: u64) -> u64 {
        let reader = row.as_reader(self.schema.as_type_ids());
        reader.get_u64(self.column_index).unwrap()
    }
}
