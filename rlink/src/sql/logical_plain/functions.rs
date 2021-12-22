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

use crate::core::data_types::DataType;
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
                )))
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
            Signature::variadic(vec![DataType::String], fun.volatility())
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
            vec![DataType::String, DataType::String],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Btrim
        | BuiltinScalarFunction::Ltrim
        | BuiltinScalarFunction::Rtrim => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::String]),
                TypeSignature::Exact(vec![DataType::String, DataType::String]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Chr | BuiltinScalarFunction::ToHex => {
            Signature::uniform(1, vec![DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::Lpad | BuiltinScalarFunction::Rpad => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::String, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::String, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::String, DataType::Int64, DataType::String]),
                TypeSignature::Exact(vec![DataType::String, DataType::Int64, DataType::String]),
                TypeSignature::Exact(vec![DataType::String, DataType::Int64, DataType::String]),
                TypeSignature::Exact(vec![DataType::String, DataType::Int64, DataType::String]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::Left
        | BuiltinScalarFunction::Repeat
        | BuiltinScalarFunction::Right => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::String, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::String, DataType::Int64]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::ToTimestamp => {
            Signature::uniform(1, vec![DataType::String, DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::ToTimestampMillis => {
            Signature::uniform(1, vec![DataType::String, DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::ToTimestampMicros => {
            Signature::uniform(1, vec![DataType::String, DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::ToTimestampSeconds => {
            Signature::uniform(1, vec![DataType::String, DataType::Int64], fun.volatility())
        }
        BuiltinScalarFunction::Digest => {
            Signature::exact(vec![DataType::String, DataType::String], fun.volatility())
        }
        BuiltinScalarFunction::SplitPart => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::String, DataType::String, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::String, DataType::String, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::String, DataType::String, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::String, DataType::String, DataType::Int64]),
            ],
            fun.volatility(),
        ),

        BuiltinScalarFunction::Strpos | BuiltinScalarFunction::StartsWith => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::String, DataType::String]),
                TypeSignature::Exact(vec![DataType::String, DataType::String]),
                TypeSignature::Exact(vec![DataType::String, DataType::String]),
                TypeSignature::Exact(vec![DataType::String, DataType::String]),
            ],
            fun.volatility(),
        ),

        BuiltinScalarFunction::Substr => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::String, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::String, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::String, DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::String, DataType::Int64, DataType::Int64]),
            ],
            fun.volatility(),
        ),

        BuiltinScalarFunction::Replace | BuiltinScalarFunction::Translate => Signature::one_of(
            vec![TypeSignature::Exact(vec![
                DataType::String,
                DataType::String,
                DataType::String,
            ])],
            fun.volatility(),
        ),
        BuiltinScalarFunction::RegexpReplace => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::String, DataType::String, DataType::String]),
                TypeSignature::Exact(vec![
                    DataType::String,
                    DataType::String,
                    DataType::String,
                    DataType::String,
                ]),
            ],
            fun.volatility(),
        ),
        BuiltinScalarFunction::RegexpMatch => Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::String, DataType::String]),
                TypeSignature::Exact(vec![DataType::String, DataType::String]),
                TypeSignature::Exact(vec![DataType::String, DataType::String, DataType::String]),
                TypeSignature::Exact(vec![DataType::String, DataType::String, DataType::String]),
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
