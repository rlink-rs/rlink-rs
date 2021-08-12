use crate::core::checkpoint::CheckpointFunction;
use crate::core::data_types::Schema;
use crate::core::element::{FnSchema, Record};
use crate::core::function::{Context, KeySelectorFunction, NamedFunction};

#[derive(Debug)]
pub struct SchemaKeySelector {
    schema: Schema,
    key_schema: Schema,
    columns: Vec<usize>,
}

impl SchemaKeySelector {
    pub fn new(columns: Vec<usize>) -> Self {
        SchemaKeySelector {
            columns,
            schema: Schema::empty(),
            key_schema: Schema::empty(),
        }
    }
}

impl KeySelectorFunction for SchemaKeySelector {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        // if `KeySelector` opened in `Reduce` operator, the `input_schema` is a Tuple
        let schema = match &context.input_schema {
            FnSchema::Single(field_types) => field_types,
            FnSchema::Tuple(field_types, _key_field_types) => field_types,
            _ => panic!("input schema not found in selector operator"),
        };

        self.schema = schema.clone();
        self.key_schema = self.key_schema(FnSchema::from(&self.schema)).into();

        Ok(())
    }

    fn get_key(&self, record: &mut Record) -> Record {
        let mut record_key = Record::with_capacity(record.len());
        let mut writer = record_key.as_writer(&self.key_schema.as_type_ids());

        let reader = record.as_reader(&self.schema.as_type_ids());

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

    fn key_schema(&self, input_schema: FnSchema) -> FnSchema {
        let schema: Schema = input_schema.into();
        let key_schema = schema.sub_schema(self.columns.as_slice());
        if key_schema.is_empty() {
            panic!("key not found");
        }

        FnSchema::Single(key_schema)
    }
}

impl NamedFunction for SchemaKeySelector {
    fn name(&self) -> &str {
        "SchemaBaseKeySelector"
    }
}

impl CheckpointFunction for SchemaKeySelector {}
