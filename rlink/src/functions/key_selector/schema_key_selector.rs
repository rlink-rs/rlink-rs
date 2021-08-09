use crate::core::checkpoint::CheckpointFunction;
use crate::core::element::Record;
use crate::core::function::{Context, KeySelectorFunction, NamedFunction};
use crate::functions::FunctionSchema;

#[derive(Debug)]
pub struct SchemaKeySelector {
    field_types: Vec<u8>,
    key_field_types: Vec<u8>,
    columns: Vec<usize>,
}

impl SchemaKeySelector {
    pub fn new(columns: Vec<usize>, data_types: &[u8]) -> Self {
        let key_field_types: Vec<u8> = columns.iter().map(|index| data_types[*index]).collect();
        SchemaKeySelector {
            columns,
            field_types: data_types.to_vec(),
            key_field_types,
        }
    }
}

impl FunctionSchema for SchemaKeySelector {
    fn schema_types(&self) -> Vec<u8> {
        self.key_field_types.clone()
    }
}

impl KeySelectorFunction for SchemaKeySelector {
    fn open(&mut self, _context: &Context) -> crate::core::Result<()> {
        Ok(())
    }

    fn get_key(&self, record: &mut Record) -> Record {
        let mut record_key = Record::with_capacity(record.len());
        let mut writer = record_key.as_writer(self.key_field_types.as_slice());

        let reader = record.as_reader(self.field_types.as_slice());

        for index in 0..self.columns.len() {
            writer
                .set_bytes_raw(reader.get_bytes_raw(self.columns[index]).unwrap())
                .unwrap();
        }

        record_key
    }

    fn close(&mut self) -> crate::core::Result<()> {
        Ok(())
    }
}

impl NamedFunction for SchemaKeySelector {
    fn name(&self) -> &str {
        "SchemaBaseKeySelector"
    }
}

impl CheckpointFunction for SchemaKeySelector {}
