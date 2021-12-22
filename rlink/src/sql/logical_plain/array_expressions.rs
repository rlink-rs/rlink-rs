use crate::core::data_types::DataType;

/// Currently supported types by the array function.
/// The order of these types correspond to the order on which coercion applies
/// This should thus be from least informative to most informative
pub static SUPPORTED_ARRAY_TYPES: &[DataType] = &[
    DataType::Boolean,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
    DataType::String,
];
