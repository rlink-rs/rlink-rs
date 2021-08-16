use crate::core::data_types::{Field, Schema};

#[derive(Clone, Debug)]
pub enum ColumnLocate {
    Index(usize),
    Name(String),
}

impl ColumnLocate {
    pub fn to_column<'a>(&self, schema: &'a Schema) -> (usize, &'a Field) {
        match self {
            Self::Index(index) => (*index, schema.field(*index)),
            Self::Name(name) => schema.column_with_name(name.as_str()).unwrap(),
        }
    }
}

pub trait ColumnLocateBuilder {
    fn build(&self) -> ColumnLocate;
}

impl ColumnLocateBuilder for usize {
    fn build(&self) -> ColumnLocate {
        ColumnLocate::Index(*self)
    }
}

impl<'a> ColumnLocateBuilder for &'a str {
    fn build(&self) -> ColumnLocate {
        ColumnLocate::Name(self.to_string())
    }
}
