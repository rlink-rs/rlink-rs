use crate::core::checkpoint::CheckpointFunction;
use crate::core::data_types::{Field, Schema};
use crate::core::element::{FnSchema, Record};
use crate::core::function::{Context, KeySelectorFunction, NamedFunction};
use crate::functions::column_locate::{ColumnLocate, ColumnLocateBuilder};

#[derive(Debug)]
pub struct SchemaKeySelector {
    schema: Schema,
    key_schema: Schema,
    column_locates: Vec<ColumnLocate>,

    columns: Vec<usize>,
}

impl SchemaKeySelector {
    pub fn new<T: ColumnLocateBuilder>(columns: Vec<T>) -> Self {
        let column_locates: Vec<ColumnLocate> = columns.into_iter().map(|x| x.build()).collect();
        SchemaKeySelector {
            column_locates,
            schema: Schema::empty(),
            key_schema: Schema::empty(),
            columns: vec![],
        }
    }

    fn key_column(&self, schema: &Schema) -> (Vec<usize>, Vec<Field>) {
        let mut columns = Vec::new();
        let mut fields = Vec::new();

        for column_locate in &self.column_locates {
            let (index, field) = column_locate.to_column(&schema);
            columns.push(index);
            fields.push(field.clone());
        }

        (columns, fields)
    }
}

impl KeySelectorFunction for SchemaKeySelector {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        // if `KeySelector` opened in `Reduce` operator, the `input_schema` is a Tuple
        let schema = context.input_schema.first();

        self.schema = schema.clone();
        self.key_schema = self.key_schema(FnSchema::from(&self.schema)).into();

        let (columns, _fields) = self.key_column(&self.schema);
        self.columns = columns;

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
        let (_columns, fields) = self.key_column(&schema);

        let key_schema = Schema::new(fields);
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
