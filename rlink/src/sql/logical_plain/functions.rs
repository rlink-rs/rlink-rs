//! Declaration of built-in (scalar) functions.
//! This module contains built-in functions' enumeration and metadata.
//!
//! Generally, a function has:
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for sqrt, ([f32]) -> f32, ([f64]) -> f64.
//!
//! This module also has a set of coercion rules to improve user experience: if an argument i32 is passed
//! to a function that supports f64, it is coerced to f64.

use std::str::FromStr;
use std::sync::Arc;

use crate::core::data_types::{DataType, Field};
use crate::sql::error::DataFusionError;
use crate::sql::logical_plain::array_expressions;

/// A function's type signature, which defines the function's supported argument types.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum TypeSignature {
    /// arbitrary number of arguments of an common type out of a list of valid types
    // A function such as `concat` is `Variadic(vec![DataType::Utf8, DataType::LargeUtf8])`
    // 表示该函数参数可以接受Vec<DataType>范围内的类型，数量不固定
    Variadic(Vec<DataType>),
    /// arbitrary number of arguments of an arbitrary but equal type
    // A function such as `array` is `VariadicEqual`
    // The first argument decides the type used for coercion
    // 表示该函数参数类型必须是一致的，数量不固定
    VariadicEqual,
    /// fixed number of arguments of an arbitrary but equal type out of a list of valid types
    // A function of one argument of f64 is `Uniform(1, vec![DataType::Float64])`
    // A function of one argument of f64 or f32 is `Uniform(1, vec![DataType::Float32, DataType::Float64])`
    // 表示该函数需要n:usize个参数，参数类型在Vec<DataType>范围内，
    // 用来声明avg,sum,max等函数，它们接受参数是固定的，但可以对很多类型进行计算，比如avg(i32),avg(i64)
    Uniform(usize, Vec<DataType>),
    /// exact number of arguments of an exact type
    // 表示该函数接受的参数类型和数量是精确固定的
    Exact(Vec<DataType>),
    /// fixed number of arguments of arbitrary types
    // 表示该函数参数固定，类型不限制
    Any(usize),
    /// One of a list of signatures
    OneOf(Vec<TypeSignature>),
}

///The Signature of a function defines its supported input types as well as its volatility.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Signature {
    /// type_signature - The types that the function accepts. See [TypeSignature] for more information.
    pub type_signature: TypeSignature,
    /// volatility - The volatility of the function. See [Volatility] for more information.
    pub volatility: Volatility,
}

impl Signature {
    /// new - Creates a new Signature from any type signature and the volatility.
    pub fn new(type_signature: TypeSignature, volatility: Volatility) -> Self {
        Signature {
            type_signature,
            volatility,
        }
    }
    /// variadic - Creates a variadic signature that represents an arbitrary number of arguments all from a type in common_types.
    pub fn variadic(common_types: Vec<DataType>, volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::Variadic(common_types),
            volatility,
        }
    }
    /// variadic_equal - Creates a variadic signature that represents an arbitrary number of arguments of the same type.
    pub fn variadic_equal(volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::VariadicEqual,
            volatility,
        }
    }
    /// uniform - Creates a function with a fixed number of arguments of the same type, which must be from valid_types.
    pub fn uniform(arg_count: usize, valid_types: Vec<DataType>, volatility: Volatility) -> Self {
        Self {
            type_signature: TypeSignature::Uniform(arg_count, valid_types),
            volatility,
        }
    }
    /// exact - Creates a signture which must match the types in exact_types in order.
    pub fn exact(exact_types: Vec<DataType>, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::Exact(exact_types),
            volatility,
        }
    }
    /// any - Creates a signature which can a be made of any type but of a specified number
    pub fn any(arg_count: usize, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::Any(arg_count),
            volatility,
        }
    }
    /// one_of Creates a signature which can match any of the [TypeSignature]s which are passed in.
    pub fn one_of(type_signatures: Vec<TypeSignature>, volatility: Volatility) -> Self {
        Signature {
            type_signature: TypeSignature::OneOf(type_signatures),
            volatility,
        }
    }
}

impl TypeSignature {
    /// Returns the data types that each argument must be coerced to match
    /// `signature`.
    ///
    /// See the module level documentation for more detail on coercion.
    pub fn data_types(
        &self,
        current_types: &[DataType],
    ) -> crate::sql::error::Result<Vec<DataType>> {
        if current_types.is_empty() {
            return Ok(vec![]);
        }
        let valid_types = self.get_valid_types(current_types)?;

        if valid_types
            .iter()
            .any(|data_type| data_type == current_types)
        {
            return Ok(current_types.to_vec());
        }

        for valid_types in valid_types {
            if let Some(types) = self.maybe_data_types(&valid_types, current_types) {
                return Ok(types);
            }
        }

        // none possible -> Error
        Err(DataFusionError::Plan(format!(
            "Coercion from {:?} to the signature {:?} failed.",
            current_types, &self.type_signature
        )))
    }

    fn get_valid_types(
        &self,
        current_types: &[DataType],
    ) -> crate::sql::error::Result<Vec<Vec<DataType>>> {
        let valid_types = match self {
            TypeSignature::Variadic(valid_types) => valid_types
                .iter()
                .map(|valid_type| current_types.iter().map(|_| valid_type.clone()).collect())
                .collect(),
            TypeSignature::Uniform(number, valid_types) => valid_types
                .iter()
                .map(|valid_type| (0..*number).map(|_| valid_type.clone()).collect())
                .collect(),
            TypeSignature::VariadicEqual => {
                // one entry with the same len as current_types, whose type is `current_types[0]`.
                vec![current_types
                    .iter()
                    .map(|_| current_types[0].clone())
                    .collect()]
            }
            TypeSignature::Exact(valid_types) => vec![valid_types.clone()],
            TypeSignature::Any(number) => {
                if current_types.len() != *number {
                    return Err(DataFusionError::Plan(format!(
                        "The function expected {} arguments but received {}",
                        number,
                        current_types.len()
                    )));
                }
                vec![(0..*number).map(|i| current_types[i].clone()).collect()]
            }
            TypeSignature::OneOf(types) => types
                .iter()
                .filter_map(|t| get_valid_types(t, current_types).ok())
                .flatten()
                .collect::<Vec<_>>(),
        };

        Ok(valid_types)
    }

    /// Try to coerce current_types into valid_types.
    fn maybe_data_types(
        valid_types: &[DataType],
        current_types: &[DataType],
    ) -> Option<Vec<DataType>> {
        if valid_types.len() != current_types.len() {
            return None;
        }

        let mut new_type = Vec::with_capacity(valid_types.len());
        for (i, valid_type) in valid_types.iter().enumerate() {
            let current_type = &current_types[i];

            if current_type == valid_type {
                new_type.push(current_type.clone())
            } else {
                // attempt to coerce
                if can_coerce_from(valid_type, current_type) {
                    new_type.push(valid_type.clone())
                } else {
                    // not possible
                    return None;
                }
            }
        }
        Some(new_type)
    }

    /// Return true if a value of type `type_from` can be coerced
    /// (losslessly converted) into a value of `type_to`
    ///
    /// See the module level documentation for more detail on coercion.
    pub fn can_coerce_from(type_into: &DataType, type_from: &DataType) -> bool {
        use self::DataType::*;
        match type_into {
            Int8 => matches!(type_from, Int8),
            Int16 => matches!(type_from, Int8 | Int16 | UInt8),
            Int32 => matches!(type_from, Int8 | Int16 | Int32 | UInt8 | UInt16),
            Int64 => matches!(
                type_from,
                Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32
            ),
            UInt8 => matches!(type_from, UInt8),
            UInt16 => matches!(type_from, UInt8 | UInt16),
            UInt32 => matches!(type_from, UInt8 | UInt16 | UInt32),
            UInt64 => matches!(type_from, UInt8 | UInt16 | UInt32 | UInt64),
            Float32 => matches!(
                type_from,
                Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32
            ),
            Float64 => matches!(
                type_from,
                Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Float64
            ),
            String => true,
            _ => false,
        }
    }
}

///A function's volatility, which defines the functions eligibility for certain optimizations
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub enum Volatility {
    /// Immutable - An immutable function will always return the same output when given the same input. An example of this is [BuiltinScalarFunction::Cos].
    Immutable,
    /// Stable - A stable function may return different values given the same input accross different queries but must return the same value for a given input within a query. An example of this is [BuiltinScalarFunction::Now].
    Stable,
    /// Volatile - A volatile function may change the return value from evaluation to evaluation. Mutiple invocations of a volatile function may return different results when used in the same query. An example of this is [BuiltinScalarFunction::Random].
    Volatile,
}

/// Scalar function
///
/// The Fn param is the wrapped function but be aware that the function will
/// be passed with the slice / vec of columnar values (either scalar or array)
/// with the exception of zero param function, where a singular element vec
/// will be passed. In that case the single element is a null array to indicate
/// the batch's row count (so that the generative zero-argument function can know
/// the result array size).
pub type ScalarFunctionImplementation =
    Arc<dyn Fn(&[ColumnarValue]) -> crate::sql::error::Result<ColumnarValue> + Send + Sync>;

/// A function's return type
pub type ReturnTypeFunction =
    Arc<dyn Fn(&[DataType]) -> crate::sql::error::Result<Arc<DataType>> + Send + Sync>;

/// Enum of all built-in scalar functions
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum BuiltinScalarFunction {
    // math functions
    /// abs
    Abs,
    /// acos
    Acos,
    /// asin
    Asin,
    /// atan
    Atan,
    /// ceil
    Ceil,
    /// cos
    Cos,
    /// Digest
    Digest,
    /// exp
    Exp,
    /// floor
    Floor,
    /// ln, Natural logarithm
    Ln,
    /// log, same as log10
    Log,
    /// log10
    Log10,
    /// log2
    Log2,
    /// round
    Round,
    /// signum
    Signum,
    /// sin
    Sin,
    /// sqrt
    Sqrt,
    /// tan
    Tan,
    /// trunc
    Trunc,

    // string functions
    /// construct an array from columns
    Array,
    /// ascii
    Ascii,
    /// bit_length
    BitLength,
    /// btrim
    Btrim,
    /// character_length
    CharacterLength,
    /// chr
    Chr,
    /// concat
    Concat,
    /// concat_ws
    ConcatWithSeparator,
    /// date_part
    DatePart,
    /// date_trunc
    DateTrunc,
    /// initcap
    InitCap,
    /// left
    Left,
    /// lpad
    Lpad,
    /// lower
    Lower,
    /// ltrim
    Ltrim,
    /// md5
    MD5,
    /// nullif
    NullIf,
    /// octet_length
    OctetLength,
    /// random
    Random,
    /// regexp_replace
    RegexpReplace,
    /// repeat
    Repeat,
    /// replace
    Replace,
    /// reverse
    Reverse,
    /// right
    Right,
    /// rpad
    Rpad,
    /// rtrim
    Rtrim,
    /// sha224
    SHA224,
    /// sha256
    SHA256,
    /// sha384
    SHA384,
    /// Sha512
    SHA512,
    /// split_part
    SplitPart,
    /// starts_with
    StartsWith,
    /// strpos
    Strpos,
    /// substr
    Substr,
    /// to_hex
    ToHex,
    /// to_timestamp
    ToTimestamp,
    /// to_timestamp_millis
    ToTimestampMillis,
    /// to_timestamp_micros
    ToTimestampMicros,
    /// to_timestamp_seconds
    ToTimestampSeconds,
    ///now
    Now,
    /// translate
    Translate,
    /// trim
    Trim,
    /// upper
    Upper,
    /// regexp_match
    RegexpMatch,
}

impl BuiltinScalarFunction {
    /// an allowlist of functions to take zero arguments, so that they will get special treatment
    /// while executing.
    fn supports_zero_argument(&self) -> bool {
        matches!(
            self,
            BuiltinScalarFunction::Random | BuiltinScalarFunction::Now
        )
    }
    /// Returns the [Volatility] of the builtin function.
    pub fn volatility(&self) -> Volatility {
        match self {
            //Immutable scalar builtins
            BuiltinScalarFunction::Abs => Volatility::Immutable,
            BuiltinScalarFunction::Acos => Volatility::Immutable,
            BuiltinScalarFunction::Asin => Volatility::Immutable,
            BuiltinScalarFunction::Atan => Volatility::Immutable,
            BuiltinScalarFunction::Ceil => Volatility::Immutable,
            BuiltinScalarFunction::Cos => Volatility::Immutable,
            BuiltinScalarFunction::Exp => Volatility::Immutable,
            BuiltinScalarFunction::Floor => Volatility::Immutable,
            BuiltinScalarFunction::Ln => Volatility::Immutable,
            BuiltinScalarFunction::Log => Volatility::Immutable,
            BuiltinScalarFunction::Log10 => Volatility::Immutable,
            BuiltinScalarFunction::Log2 => Volatility::Immutable,
            BuiltinScalarFunction::Round => Volatility::Immutable,
            BuiltinScalarFunction::Signum => Volatility::Immutable,
            BuiltinScalarFunction::Sin => Volatility::Immutable,
            BuiltinScalarFunction::Sqrt => Volatility::Immutable,
            BuiltinScalarFunction::Tan => Volatility::Immutable,
            BuiltinScalarFunction::Trunc => Volatility::Immutable,
            BuiltinScalarFunction::Array => Volatility::Immutable,
            BuiltinScalarFunction::Ascii => Volatility::Immutable,
            BuiltinScalarFunction::BitLength => Volatility::Immutable,
            BuiltinScalarFunction::Btrim => Volatility::Immutable,
            BuiltinScalarFunction::CharacterLength => Volatility::Immutable,
            BuiltinScalarFunction::Chr => Volatility::Immutable,
            BuiltinScalarFunction::Concat => Volatility::Immutable,
            BuiltinScalarFunction::ConcatWithSeparator => Volatility::Immutable,
            BuiltinScalarFunction::DatePart => Volatility::Immutable,
            BuiltinScalarFunction::DateTrunc => Volatility::Immutable,
            BuiltinScalarFunction::InitCap => Volatility::Immutable,
            BuiltinScalarFunction::Left => Volatility::Immutable,
            BuiltinScalarFunction::Lpad => Volatility::Immutable,
            BuiltinScalarFunction::Lower => Volatility::Immutable,
            BuiltinScalarFunction::Ltrim => Volatility::Immutable,
            BuiltinScalarFunction::MD5 => Volatility::Immutable,
            BuiltinScalarFunction::NullIf => Volatility::Immutable,
            BuiltinScalarFunction::OctetLength => Volatility::Immutable,
            BuiltinScalarFunction::RegexpReplace => Volatility::Immutable,
            BuiltinScalarFunction::Repeat => Volatility::Immutable,
            BuiltinScalarFunction::Replace => Volatility::Immutable,
            BuiltinScalarFunction::Reverse => Volatility::Immutable,
            BuiltinScalarFunction::Right => Volatility::Immutable,
            BuiltinScalarFunction::Rpad => Volatility::Immutable,
            BuiltinScalarFunction::Rtrim => Volatility::Immutable,
            BuiltinScalarFunction::SHA224 => Volatility::Immutable,
            BuiltinScalarFunction::SHA256 => Volatility::Immutable,
            BuiltinScalarFunction::SHA384 => Volatility::Immutable,
            BuiltinScalarFunction::SHA512 => Volatility::Immutable,
            BuiltinScalarFunction::Digest => Volatility::Immutable,
            BuiltinScalarFunction::SplitPart => Volatility::Immutable,
            BuiltinScalarFunction::StartsWith => Volatility::Immutable,
            BuiltinScalarFunction::Strpos => Volatility::Immutable,
            BuiltinScalarFunction::Substr => Volatility::Immutable,
            BuiltinScalarFunction::ToHex => Volatility::Immutable,
            BuiltinScalarFunction::ToTimestamp => Volatility::Immutable,
            BuiltinScalarFunction::ToTimestampMillis => Volatility::Immutable,
            BuiltinScalarFunction::ToTimestampMicros => Volatility::Immutable,
            BuiltinScalarFunction::ToTimestampSeconds => Volatility::Immutable,
            BuiltinScalarFunction::Translate => Volatility::Immutable,
            BuiltinScalarFunction::Trim => Volatility::Immutable,
            BuiltinScalarFunction::Upper => Volatility::Immutable,
            BuiltinScalarFunction::RegexpMatch => Volatility::Immutable,

            //Stable builtin functions
            // eg: The now function, although variable, does not change after executing once.
            // It s just that every sql execution is changing.
            BuiltinScalarFunction::Now => Volatility::Stable,

            //Volatile builtin functions
            // eg: The random function, in the sql execution, each execution changes.
            BuiltinScalarFunction::Random => Volatility::Volatile,
        }
    }
}

impl std::fmt::Display for BuiltinScalarFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // lowercase of the debug.
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

impl FromStr for BuiltinScalarFunction {
    type Err = DataFusionError;
    fn from_str(name: &str) -> crate::sql::error::Result<BuiltinScalarFunction> {
        Ok(match name {
            // math functions
            "abs" => BuiltinScalarFunction::Abs,
            "acos" => BuiltinScalarFunction::Acos,
            "asin" => BuiltinScalarFunction::Asin,
            "atan" => BuiltinScalarFunction::Atan,
            "ceil" => BuiltinScalarFunction::Ceil,
            "cos" => BuiltinScalarFunction::Cos,
            "exp" => BuiltinScalarFunction::Exp,
            "floor" => BuiltinScalarFunction::Floor,
            "ln" => BuiltinScalarFunction::Ln,
            "log" => BuiltinScalarFunction::Log,
            "log10" => BuiltinScalarFunction::Log10,
            "log2" => BuiltinScalarFunction::Log2,
            "round" => BuiltinScalarFunction::Round,
            "signum" => BuiltinScalarFunction::Signum,
            "sin" => BuiltinScalarFunction::Sin,
            "sqrt" => BuiltinScalarFunction::Sqrt,
            "tan" => BuiltinScalarFunction::Tan,
            "trunc" => BuiltinScalarFunction::Trunc,

            // string functions
            "array" => BuiltinScalarFunction::Array,
            "ascii" => BuiltinScalarFunction::Ascii,
            "bit_length" => BuiltinScalarFunction::BitLength,
            "btrim" => BuiltinScalarFunction::Btrim,
            "char_length" => BuiltinScalarFunction::CharacterLength,
            "character_length" => BuiltinScalarFunction::CharacterLength,
            "concat" => BuiltinScalarFunction::Concat,
            "concat_ws" => BuiltinScalarFunction::ConcatWithSeparator,
            "chr" => BuiltinScalarFunction::Chr,
            "date_part" | "datepart" => BuiltinScalarFunction::DatePart,
            "date_trunc" | "datetrunc" => BuiltinScalarFunction::DateTrunc,
            "initcap" => BuiltinScalarFunction::InitCap,
            "left" => BuiltinScalarFunction::Left,
            "length" => BuiltinScalarFunction::CharacterLength,
            "lower" => BuiltinScalarFunction::Lower,
            "lpad" => BuiltinScalarFunction::Lpad,
            "ltrim" => BuiltinScalarFunction::Ltrim,
            "md5" => BuiltinScalarFunction::MD5,
            "nullif" => BuiltinScalarFunction::NullIf,
            "octet_length" => BuiltinScalarFunction::OctetLength,
            "random" => BuiltinScalarFunction::Random,
            "regexp_replace" => BuiltinScalarFunction::RegexpReplace,
            "repeat" => BuiltinScalarFunction::Repeat,
            "replace" => BuiltinScalarFunction::Replace,
            "reverse" => BuiltinScalarFunction::Reverse,
            "right" => BuiltinScalarFunction::Right,
            "rpad" => BuiltinScalarFunction::Rpad,
            "rtrim" => BuiltinScalarFunction::Rtrim,
            "sha224" => BuiltinScalarFunction::SHA224,
            "sha256" => BuiltinScalarFunction::SHA256,
            "sha384" => BuiltinScalarFunction::SHA384,
            "sha512" => BuiltinScalarFunction::SHA512,
            "digest" => BuiltinScalarFunction::Digest,
            "split_part" => BuiltinScalarFunction::SplitPart,
            "starts_with" => BuiltinScalarFunction::StartsWith,
            "strpos" => BuiltinScalarFunction::Strpos,
            "substr" => BuiltinScalarFunction::Substr,
            "to_hex" => BuiltinScalarFunction::ToHex,
            "to_timestamp" => BuiltinScalarFunction::ToTimestamp,
            "to_timestamp_millis" => BuiltinScalarFunction::ToTimestampMillis,
            "to_timestamp_micros" => BuiltinScalarFunction::ToTimestampMicros,
            "to_timestamp_seconds" => BuiltinScalarFunction::ToTimestampSeconds,
            "now" => BuiltinScalarFunction::Now,
            "translate" => BuiltinScalarFunction::Translate,
            "trim" => BuiltinScalarFunction::Trim,
            "upper" => BuiltinScalarFunction::Upper,
            "regexp_match" => BuiltinScalarFunction::RegexpMatch,
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "There is no built-in function named {}",
                    name
                )));
            }
        })
    }
}

/// the signatures supported by the function `fun`.
fn signature(fun: &BuiltinScalarFunction) -> Signature {
    // note: the physical expression must accept the type returned by this function or the execution panics.

    // for now, the list is small, as we do not have many built-in functions.
    match fun {
        BuiltinScalarFunction::Array => Signature::variadic(
            array_expressions::SUPPORTED_ARRAY_TYPES.to_vec(),
            fun.volatility(),
        ),
        BuiltinScalarFunction::Concat | BuiltinScalarFunction::ConcatWithSeparator => {
            Signature::variadic(vec![DataType::Utf8], fun.volatility())
        }
        BuiltinScalarFunction::Ascii
        | BuiltinScalarFunction::BitLength
        | BuiltinScalarFunction::CharacterLength
        | BuiltinScalarFunction::InitCap
        | BuiltinScalarFunction::Lower
        | BuiltinScalarFunction::MD5
        | BuiltinScalarFunction::OctetLength
        | BuiltinScalarFunction::Reverse
        | BuiltinScalarFunction::SHA224
        | BuiltinScalarFunction::SHA256
        | BuiltinScalarFunction::SHA384
        | BuiltinScalarFunction::SHA512
        | BuiltinScalarFunction::Trim
        | BuiltinScalarFunction::Upper => Signature::uniform(
            1,
            vec![DataType::Utf8, DataType::LargeUtf8],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Btrim
        | BuiltinScalarFunction::Ltrim
        | BuiltinScalarFunction::Rtrim => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Chr | BuiltinScalarFunction::ToHex => {
            Signature::uniform(1, vec![DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::Lpad | BuiltinScalarFunction::Rpad => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Int64, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64, DataType::LargeUtf8]),
                TypeSignature::Exact(vec![
                    DataType::LargeUtf8,
                    DataType::Int64,
                    DataType::LargeUtf8,
                ]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Left
        | BuiltinScalarFunction::Repeat
        | BuiltinScalarFunction::Right => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Int64]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::ToTimestamp => {
            Signature::uniform(1, vec![DataType::Utf8, DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::ToTimestampMillis => {
            Signature::uniform(1, vec![DataType::Utf8, DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::ToTimestampMicros => {
            Signature::uniform(1, vec![DataType::Utf8, DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::ToTimestampSeconds => {
            Signature::uniform(1, vec![DataType::Utf8, DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::Digest => {
            Signature::exact(vec![DataType::Utf8, DataType::Utf8], fun.volatility())
        }
        // BuiltinScalarFunction::DateTrunc => Signature::exact(
        //     vec![
        //         DataType::Utf8,
        //         DataType::Timestamp(TimeUnit::Nanosecond, None),
        //     ],
        //     fun.volatility(),
        // ),
        // BuiltinScalarFunction::DatePart => Signature::one_of(
        //     vec![
        //         TypeSignature::Exact(vec![DataType::Utf8, DataType::Date32]),
        //         TypeSignature::Exact(vec![DataType::Utf8, DataType::Date64]),
        //         TypeSignature::Exact(vec![
        //             DataType::Utf8,
        //             DataType::Timestamp(TimeUnit::Second, None),
        //         ]),
        //         TypeSignature::Exact(vec![
        //             DataType::Utf8,
        //             DataType::Timestamp(TimeUnit::Microsecond, None),
        //         ]),
        //         TypeSignature::Exact(vec![
        //             DataType::Utf8,
        //             DataType::Timestamp(TimeUnit::Millisecond, None),
        //         ]),
        //         TypeSignature::Exact(vec![
        //             DataType::Utf8,
        //             DataType::Timestamp(TimeUnit::Nanosecond, None),
        //         ]),
        //     ],
        //     fun.volatility(),
        // ),
        BuiltinScalarFunction::SplitPart => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::LargeUtf8, DataType::Int64]),
                TypeSignature::Exact(vec![
                    DataType::LargeUtf8,
                    DataType::LargeUtf8,
                    DataType::Int64,
                ]),
            ],
            fun.volatility(),
        ),

        BuiltinScalarFunction::Strpos | BuiltinScalarFunction::StartsWith => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::LargeUtf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::LargeUtf8]),
            ],
            fun.volatility(),
        ),

        BuiltinScalarFunction::Substr => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Int64, DataType::Int64]),
            ],
            fun.volatility(),
        ),

        BuiltinScalarFunction::Replace | BuiltinScalarFunction::Translate => Signature::one_of(
            vec![TypeSignature::Exact(vec![
                DataType::Utf8,
                DataType::Utf8,
                DataType::Utf8,
            ])],
            fun.volatility(),
        ),
        BuiltinScalarFunction::RegexpReplace => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                ]),
            ],
            fun.volatility(),
        ),

        BuiltinScalarFunction::NullIf => {
            Signature::uniform(2, SUPPORTED_NULLIF_TYPES.to_vec(), fun.volatility())
        }
        BuiltinScalarFunction::RegexpMatch => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8, DataType::Utf8]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Random => Signature::exact(vec![], fun.volatility()),
        // math expressions expect 1 argument of type f64 or f32
        // priority is given to f64 because e.g. `sqrt(1i32)` is in IR (real numbers) and thus we
        // return the best approximation for it (in f64).
        // We accept f32 because in this case it is clear that the best approximation
        // will be as good as the number of digits in the number
        _ => Signature::uniform(
            1,
            vec![DataType::Float64, DataType::Float32],
            fun.volatility(),
        ),
    }
}

macro_rules! make_utf8_to_return_type {
    ($FUNC:ident, $largeUtf8Type:expr, $utf8Type:expr) => {
        fn $FUNC(arg_type: &DataType, name: &str) -> Result<DataType> {
            Ok(match arg_type {
                DataType::LargeUtf8 => $largeUtf8Type,
                DataType::Utf8 => $utf8Type,
                _ => {
                    // this error is internal as `data_types` should have captured this.
                    return Err(DataFusionError::Internal(format!(
                        "The {:?} function can only accept strings.",
                        name
                    )));
                }
            })
        }
    };
}

make_utf8_to_return_type!(utf8_to_str_type, DataType::LargeUtf8, DataType::Utf8);
make_utf8_to_return_type!(utf8_to_int_type, DataType::Int64, DataType::Int32);
make_utf8_to_return_type!(utf8_to_binary_type, DataType::Binary, DataType::Binary);

/// Returns the datatype of the scalar function
pub fn return_type(
    fun: &BuiltinScalarFunction,
    input_expr_types: &[DataType],
) -> crate::sql::error::Result<DataType> {
    // Note that this function *must* return the same type that the respective physical expression returns
    // or the execution panics.

    if input_expr_types.is_empty() && !fun.supports_zero_argument() {
        return Err(DataFusionError::Internal(format!(
            "Builtin scalar function {} does not support empty arguments",
            fun
        )));
    }

    // verify that this is a valid set of data types for this function
    data_types(input_expr_types, &signature(fun))?;

    // the return type of the built in function.
    // Some built-in functions' return type depends on the incoming type.
    match fun {
        // BuiltinScalarFunction::Array => Ok(DataType::FixedSizeList(
        //     Box::new(Field::new("item", input_expr_types[0].clone())),
        //     input_expr_types.len() as i32,
        // )),
        BuiltinScalarFunction::Ascii => Ok(DataType::Int32),
        BuiltinScalarFunction::BitLength => utf8_to_int_type(&input_expr_types[0], "bit_length"),
        BuiltinScalarFunction::Btrim => utf8_to_str_type(&input_expr_types[0], "btrim"),
        BuiltinScalarFunction::CharacterLength => {
            utf8_to_int_type(&input_expr_types[0], "character_length")
        }
        BuiltinScalarFunction::Chr => Ok(DataType::String),
        BuiltinScalarFunction::Concat => Ok(DataType::String),
        BuiltinScalarFunction::ConcatWithSeparator => Ok(DataType::String),
        BuiltinScalarFunction::DatePart => Ok(DataType::Int32),
        BuiltinScalarFunction::DateTrunc => Ok(DataType::UInt64),
        BuiltinScalarFunction::InitCap => utf8_to_str_type(&input_expr_types[0], "initcap"),
        BuiltinScalarFunction::Left => utf8_to_str_type(&input_expr_types[0], "left"),
        BuiltinScalarFunction::Lower => utf8_to_str_type(&input_expr_types[0], "lower"),
        BuiltinScalarFunction::Lpad => utf8_to_str_type(&input_expr_types[0], "lpad"),
        BuiltinScalarFunction::Ltrim => utf8_to_str_type(&input_expr_types[0], "ltrim"),
        BuiltinScalarFunction::MD5 => utf8_to_str_type(&input_expr_types[0], "md5"),
        BuiltinScalarFunction::NullIf => {
            // NULLIF has two args and they might get coerced, get a preview of this
            let coerced_types = data_types(input_expr_types, &signature(fun));
            coerced_types.map(|typs| typs[0].clone())
        }
        BuiltinScalarFunction::OctetLength => {
            utf8_to_int_type(&input_expr_types[0], "octet_length")
        }
        BuiltinScalarFunction::Random => Ok(DataType::Float64),
        BuiltinScalarFunction::RegexpReplace => {
            utf8_to_str_type(&input_expr_types[0], "regex_replace")
        }
        BuiltinScalarFunction::Repeat => utf8_to_str_type(&input_expr_types[0], "repeat"),
        BuiltinScalarFunction::Replace => utf8_to_str_type(&input_expr_types[0], "replace"),
        BuiltinScalarFunction::Reverse => utf8_to_str_type(&input_expr_types[0], "reverse"),
        BuiltinScalarFunction::Right => utf8_to_str_type(&input_expr_types[0], "right"),
        BuiltinScalarFunction::Rpad => utf8_to_str_type(&input_expr_types[0], "rpad"),
        BuiltinScalarFunction::Rtrim => utf8_to_str_type(&input_expr_types[0], "rtrimp"),
        BuiltinScalarFunction::SHA224 => utf8_to_binary_type(&input_expr_types[0], "sha224"),
        BuiltinScalarFunction::SHA256 => utf8_to_binary_type(&input_expr_types[0], "sha256"),
        BuiltinScalarFunction::SHA384 => utf8_to_binary_type(&input_expr_types[0], "sha384"),
        BuiltinScalarFunction::SHA512 => utf8_to_binary_type(&input_expr_types[0], "sha512"),
        BuiltinScalarFunction::Digest => utf8_to_binary_type(&input_expr_types[0], "digest"),
        BuiltinScalarFunction::SplitPart => utf8_to_str_type(&input_expr_types[0], "split_part"),
        BuiltinScalarFunction::StartsWith => Ok(DataType::Boolean),
        BuiltinScalarFunction::Strpos => utf8_to_int_type(&input_expr_types[0], "strpos"),
        BuiltinScalarFunction::Substr => utf8_to_str_type(&input_expr_types[0], "substr"),
        BuiltinScalarFunction::ToHex => Ok(match input_expr_types[0] {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => DataType::Utf8,
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The to_hex function can only accept integers.".to_string(),
                ));
            }
        }),
        // BuiltinScalarFunction::ToTimestamp => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        // BuiltinScalarFunction::ToTimestampMillis => {
        //     Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
        // }
        // BuiltinScalarFunction::ToTimestampMicros => {
        //     Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        // }
        // BuiltinScalarFunction::ToTimestampSeconds => {
        //     Ok(DataType::Timestamp(TimeUnit::Second, None))
        // }
        // BuiltinScalarFunction::Now => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        BuiltinScalarFunction::Translate => utf8_to_str_type(&input_expr_types[0], "translate"),
        BuiltinScalarFunction::Trim => utf8_to_str_type(&input_expr_types[0], "trim"),
        BuiltinScalarFunction::Upper => utf8_to_str_type(&input_expr_types[0], "upper"),
        BuiltinScalarFunction::RegexpMatch => Ok(match input_expr_types[0] {
            DataType::String => DataType::List(Box::new(Field::new("item", DataType::String))),
            _ => {
                // this error is internal as `data_types` should have captured this.
                return Err(DataFusionError::Internal(
                    "The regexp_extract function can only accept strings.".to_string(),
                ));
            }
        }),

        BuiltinScalarFunction::Abs
        | BuiltinScalarFunction::Acos
        | BuiltinScalarFunction::Asin
        | BuiltinScalarFunction::Atan
        | BuiltinScalarFunction::Ceil
        | BuiltinScalarFunction::Cos
        | BuiltinScalarFunction::Exp
        | BuiltinScalarFunction::Floor
        | BuiltinScalarFunction::Log
        | BuiltinScalarFunction::Ln
        | BuiltinScalarFunction::Log10
        | BuiltinScalarFunction::Log2
        | BuiltinScalarFunction::Round
        | BuiltinScalarFunction::Signum
        | BuiltinScalarFunction::Sin
        | BuiltinScalarFunction::Sqrt
        | BuiltinScalarFunction::Tan
        | BuiltinScalarFunction::Trunc => match input_expr_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            _ => Ok(DataType::Float64),
        },
        _ => Err(DataFusionError::NotImplemented(format!(
            "unsupported function {}",
            fun
        ))),
    }
}
