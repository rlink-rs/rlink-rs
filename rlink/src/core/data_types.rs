use std::convert::TryFrom;

use serbuffer::{types, FieldMetadata};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DataType {
    /// A boolean datatype representing the values `true` and `false`.
    Boolean,
    /// A signed 8-bit integer.
    Int8,
    /// An unsigned 8-bit integer.
    UInt8,
    /// A signed 16-bit integer.
    Int16,
    /// An unsigned 16-bit integer.
    UInt16,
    /// A signed 32-bit integer.
    Int32,
    /// An unsigned 32-bit integer.
    UInt32,
    /// A signed 64-bit integer.
    Int64,
    /// An unsigned 64-bit integer.
    UInt64,
    /// A 32-bit floating point number.
    Float32,
    /// A 64-bit floating point number.
    Float64,
    /// Opaque binary data of variable length.
    Binary,
    /// A variable-length string in Unicode with UTF-8 encoding.
    String,
}

impl DataType {
    pub fn len(&self) -> usize {
        match self {
            Self::Boolean => 1,
            Self::Int8 => 1,
            Self::UInt8 => 1,
            Self::Int16 => 2,
            Self::UInt16 => 2,
            Self::Int32 => 4,
            Self::UInt32 => 4,
            Self::Int64 => 8,
            Self::UInt64 => 8,
            Self::Float32 => 4,
            Self::Float64 => 8,
            Self::Binary => 0,
            Self::String => 0,
        }
    }

    pub fn id(&self) -> u8 {
        match self {
            Self::Boolean => types::BOOL,
            Self::Int8 => types::I8,
            Self::UInt8 => types::U8,
            Self::Int16 => types::I16,
            Self::UInt16 => types::U16,
            Self::Int32 => types::I32,
            Self::UInt32 => types::U32,
            Self::Int64 => types::I64,
            Self::UInt64 => types::U64,
            Self::Float32 => types::F32,
            Self::Float64 => types::F64,
            Self::Binary => types::BINARY,
            Self::String => types::STRING,
        }
    }
}

impl TryFrom<u8> for DataType {
    type Error = crate::core::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            types::BOOL => Ok(Self::Boolean),
            types::I8 => Ok(Self::Int8),
            types::U8 => Ok(Self::UInt8),
            types::I16 => Ok(Self::Int16),
            types::U16 => Ok(Self::UInt16),
            types::I32 => Ok(Self::Int32),
            types::U32 => Ok(Self::UInt32),
            types::I64 => Ok(Self::Int64),
            types::U64 => Ok(Self::UInt64),
            types::F32 => Ok(Self::Float32),
            types::F64 => Ok(Self::Float64),
            types::BINARY => Ok(Self::Binary),
            types::STRING => Ok(Self::String),
            _ => Err(crate::core::Error::from("unknown type")),
        }
    }
}

/// Contains the meta-data for a single relative type.
///
/// The `Schema` object is an ordered collection of `Field` objects.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Field {
    name: String,
    data_type: DataType,
    len: usize,
    type_id: u8,
}

impl Field {
    pub fn new(name: &str, data_type: DataType) -> Self {
        let len = data_type.len();
        let type_id = data_type.id();
        Field {
            name: name.to_string(),
            data_type,
            len,
            type_id,
        }
    }

    #[inline]
    pub fn data_type_id(&self) -> u8 {
        self.type_id
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_numeric(&self) -> bool {
        self.type_id < types::BINARY
    }
}

/// Describes the meta-data of an ordered sequence of relative types.
///
/// Note that this information is only part of the meta-data and not part of the physical
/// memory layout.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub(crate) fields: Vec<Field>,
    pub(crate) type_ids: Vec<u8>,
}

impl Schema {
    pub fn new(fields: Vec<Field>) -> Self {
        let type_ids: Vec<u8> = fields.iter().map(|x| x.type_id).collect();
        Self { fields, type_ids }
    }

    /// Creates an empty `Schema`
    pub fn empty() -> Self {
        Self {
            fields: vec![],
            type_ids: vec![],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    #[inline]
    pub fn as_type_ids(&self) -> &[u8] {
        self.type_ids.as_slice()
    }

    /// Returns an immutable reference of the vector of `Field` instances.
    #[inline]
    pub const fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    /// Returns an immutable reference of a specific `Field` instance selected using an
    /// offset within the internal `fields` vector.
    pub fn field(&self, i: usize) -> &Field {
        &self.fields[i]
    }

    /// Returns an immutable reference of a specific `Field` instance selected by name.
    pub fn field_with_name(&self, name: &str) -> Option<&Field> {
        self.index_of(name).map(|i| &self.fields[i])
    }

    /// Find the index of the column with the given name.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        for i in 0..self.fields.len() {
            if self.fields[i].name() == name {
                return Some(i);
            }
        }

        None
    }

    /// Look up a column by name and return a immutable reference to the column along with
    /// its index.
    pub fn column_with_name(&self, name: &str) -> Option<(usize, &Field)> {
        self.fields
            .iter()
            .enumerate()
            .find(|&(_, c)| c.name() == name)
    }

    fn add_field(&mut self, field: Field) {
        if self.index_of(field.name()).is_some() {
            warn!("name exist: {}", field.name.clone());
        }

        self.type_ids.push(field.type_id);
        self.fields.push(field);
    }

    pub fn merge(&mut self, schema: &Schema) {
        for field in &schema.fields {
            self.add_field(field.clone());
        }
    }

    pub fn sub_schema(&self, column_indies: &[usize]) -> Schema {
        let mut fields = Vec::new();
        for index in column_indies {
            let field = self.fields[*index].clone();
            fields.push(field);
        }

        Schema::new(fields)
    }
}

impl<'a, const N: usize> From<&'a FieldMetadata<N>> for Schema {
    fn from(metadata: &'a FieldMetadata<N>) -> Self {
        let field_name = metadata.field_name();
        let field_type = metadata.field_type();

        let mut fields = Vec::new();
        for n in 0..field_name.len() {
            let field = Field::new(field_name[n], DataType::try_from(field_type[n]).unwrap());

            fields.push(field);
        }

        Schema::new(fields)
    }
}
