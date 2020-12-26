use crate::api::element::Record;
use crate::api::function::Function;
use crate::api::watermark::TimestampAssigner;

#[derive(Debug)]
pub struct ColumnBaseTimestampAssigner {
    data_types: Vec<u8>,
    column: usize,
}

impl ColumnBaseTimestampAssigner {
    pub fn new(column: usize, data_types: Vec<u8>) -> Self {
        ColumnBaseTimestampAssigner { column, data_types }
    }
}

impl TimestampAssigner for ColumnBaseTimestampAssigner {
    fn extract_timestamp(&mut self, row: &mut Record, _previous_element_timestamp: u64) -> u64 {
        let mut reader = row.get_reader(self.data_types.as_slice());
        reader.get_u64(self.column).unwrap()
    }
}

impl Function for ColumnBaseTimestampAssigner {
    fn get_name(&self) -> &str {
        "ColumnBaseTimestampAssigner"
    }
}
