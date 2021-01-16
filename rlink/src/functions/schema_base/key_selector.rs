use crate::api::element::Record;
use crate::api::function::{Context, Function, KeySelectorFunction};
use crate::functions::schema_base::FunctionSchema;

#[derive(Debug)]
pub struct SchemaBaseKeySelector {
    field_types: Vec<u8>,
    key_field_types: Vec<u8>,
    columns: Vec<usize>,
}

impl SchemaBaseKeySelector {
    pub fn new(columns: Vec<usize>, data_types: &[u8]) -> Self {
        let key_field_types: Vec<u8> = columns.iter().map(|index| data_types[*index]).collect();
        SchemaBaseKeySelector {
            columns,
            field_types: data_types.to_vec(),
            key_field_types,
        }
    }
}

impl FunctionSchema for SchemaBaseKeySelector {
    fn get_schema_types(&self) -> Vec<u8> {
        self.key_field_types.clone()
    }
}

impl KeySelectorFunction for SchemaBaseKeySelector {
    fn open(&mut self, _context: &Context) {}

    fn get_key(&self, record: &mut Record) -> Record {
        let mut record_key = Record::with_capacity(record.len());
        let mut writer = record_key.get_writer(self.key_field_types.as_slice());

        let mut reader = record.get_reader(self.field_types.as_slice());

        for index in 0..self.columns.len() {
            writer
                .set_bytes_raw(reader.get_bytes_raw(self.columns[index]).unwrap())
                .unwrap();
        }

        record_key
    }

    fn close(&mut self) {}
}

impl Function for SchemaBaseKeySelector {
    fn get_name(&self) -> &str {
        "SchemaBaseKeySelector"
    }
}
