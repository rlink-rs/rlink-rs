use crate::core::checkpoint::CheckpointFunction;
use crate::core::element::{Record, Schema};
use crate::core::function::{Context, KeySelectorFunction, NamedFunction};
use crate::functions::FunctionSchema;

#[derive(Debug)]
pub struct SchemaKeySelector {
    field_types: Vec<u8>,
    key_field_types: Vec<u8>,
    columns: Vec<usize>,
}

impl SchemaKeySelector {
    pub fn new(columns: Vec<usize>) -> Self {
        SchemaKeySelector {
            columns,
            field_types: vec![],
            key_field_types: vec![],
        }
    }
}

impl FunctionSchema for SchemaKeySelector {
    fn schema_types(&self) -> Vec<u8> {
        self.key_field_types.clone()
    }
}

impl KeySelectorFunction for SchemaKeySelector {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        // if `KeySelector` opened in `Reduce` operator, the `input_schema` is a Tuple
        let field_types = match &context.input_schema {
            Schema::Single(field_types) => field_types,
            Schema::Tuple(field_types, _key_field_types) => field_types,
            _ => panic!("input schema not found in selector operator"),
        };

        self.field_types = field_types.to_vec();
        self.key_field_types = self
            .key_schema(Schema::from(self.field_types.as_slice()))
            .into();

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

    fn key_schema(&self, input_schema: Schema) -> Schema {
        let field_types: Vec<u8> = input_schema.into();
        let key_field_types: Vec<u8> = self
            .columns
            .iter()
            .map(|index| field_types[*index])
            .collect();

        if key_field_types.is_empty() {
            panic!("key not found");
        }

        Schema::Single(key_field_types)
    }
}

impl NamedFunction for SchemaKeySelector {
    fn name(&self) -> &str {
        "SchemaBaseKeySelector"
    }
}

impl CheckpointFunction for SchemaKeySelector {}
