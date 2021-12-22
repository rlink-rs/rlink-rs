use crate::core::data_types::DataType;
use std::str::FromStr;

use crate::sql::error::DataFusionError;
use crate::sql::logical_plain::functions::{Signature, Volatility};

/// Enum of all built-in aggregate functions
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum AggregateFunction {
    /// count
    Count,
    /// sum
    Sum,
    /// min
    Min,
    /// max
    Max,
    /// avg
    Avg,
    /// Approximate aggregate function
    ApproxDistinct,
    /// array_agg
    ArrayAgg,
}

impl std::fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // uppercase of the debug.
        write!(f, "{}", format!("{:?}", self).to_uppercase())
    }
}

impl FromStr for AggregateFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> crate::sql::error::Result<AggregateFunction> {
        Ok(match name {
            "min" => AggregateFunction::Min,
            "max" => AggregateFunction::Max,
            "count" => AggregateFunction::Count,
            "avg" => AggregateFunction::Avg,
            "sum" => AggregateFunction::Sum,
            "approx_distinct" => AggregateFunction::ApproxDistinct,
            "array_agg" => AggregateFunction::ArrayAgg,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "There is no built-in function named {}",
                    name
                )));
            }
        })
    }
}

static STRINGS: &[DataType] = &[DataType::String];

static NUMERICS: &[DataType] = &[
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Float32,
    DataType::Float64,
];

/// the signatures supported by the function `fun`.
pub fn signature(fun: &AggregateFunction) -> Signature {
    // note: the physical expression must accept the type returned by this function or the execution panics.
    match fun {
        AggregateFunction::Count
        | AggregateFunction::ApproxDistinct
        | AggregateFunction::ArrayAgg => Signature::any(1, Volatility::Immutable),
        AggregateFunction::Min | AggregateFunction::Max => {
            let valid = STRINGS
                .iter()
                .chain(NUMERICS.iter())
                .cloned()
                .collect::<Vec<_>>();
            Signature::uniform(1, valid, Volatility::Immutable)
        }
        AggregateFunction::Avg | AggregateFunction::Sum => {
            Signature::uniform(1, NUMERICS.to_vec(), Volatility::Immutable)
        }
    }
}
