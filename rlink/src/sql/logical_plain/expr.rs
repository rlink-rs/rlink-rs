use crate::core::data_types::DataType;
use crate::sql::logical_plain::operators::Operator;
use crate::sql::logical_plain::scalar::ScalarValue;
use crate::sql::logical_plain::window_frames::WindowFrame;

/// A named reference to a qualified field in a schema.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Column {
    /// relation/table name.
    pub relation: Option<String>,
    /// field/column name.
    pub name: String,
}

#[derive(Clone, PartialEq, PartialOrd, Debug)]
pub enum Expr {
    /// An expression with a specific name.
    Alias(Box<Expr>, String),
    /// A named reference to a qualified filed in a schema.
    Column(Column),
    /// A named reference to a variable in a registry.
    ScalarVariable(Vec<String>),
    /// A constant value.
    Literal(ScalarValue),
    /// A binary expression such as "age > 21"
    BinaryExpr {
        /// Left-hand side of the expression
        left: Box<Expr>,
        /// The comparison operator
        op: Operator,
        /// Right-hand side of the expression
        right: Box<Expr>,
    },
    /// Negation of an expression. The expression's type must be a boolean to make sense.
    Not(Box<Expr>),
    /// Whether an expression is not Null. This expression is never null.
    IsNotNull(Box<Expr>),
    /// Whether an expression is Null. This expression is never null.
    IsNull(Box<Expr>),
    /// Whether an expression is between a given range.
    Between {
        /// The value to compare
        expr: Box<Expr>,
        /// Whether the expression is negated
        negated: bool,
        /// The low end of the range
        low: Box<Expr>,
        /// The high end of the range
        high: Box<Expr>,
    },
    /// Casts the expression to a given type and will return a runtime error if the expression cannot be cast.
    /// This expression is guaranteed to have a fixed type.
    Cast {
        /// The expression being cast
        expr: Box<Expr>,
        /// The `DataType` the expression will yield
        data_type: DataType,
    },
    /// A sort expression, that can be used to sort values.
    Sort {
        /// The expression to sort on
        expr: Box<Expr>,
        /// The direction of the sort
        asc: bool,
        /// Whether to put Nulls before all other data values
        nulls_first: bool,
    },
    /// Represents the call of a built-in scalar function with a set of arguments.
    ScalarFunction {
        // /// The function
        // fun: functions::BuiltinScalarFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
    },
    /// Represents the call of an aggregate built-in function with arguments.
    AggregateFunction {
        // /// Name of the function
        // fun: aggregates::AggregateFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
        /// Whether this is a DISTINCT aggregation or not
        distinct: bool,
    },
    /// Represents the call of a window function with arguments.
    WindowFunction {
        // /// Name of the function
        // fun: window_functions::WindowFunction,
        /// List of expressions to feed to the functions as arguments
        args: Vec<Expr>,
        /// List of partition by expressions
        partition_by: Vec<Expr>,
        /// List of order by expressions
        order_by: Vec<Expr>,
        /// Window frame
        window_frame: Option<WindowFrame>,
    },
    /// Returns whether the list contains the expr value.
    InList {
        /// The expression to compare
        expr: Box<Expr>,
        /// A list of values to compare against
        list: Vec<Expr>,
        /// Whether the expression is negated
        negated: bool,
    },
    /// Represents a reference to all fields in a schema.
    Wildcard,
}

/// return a new expression l <op> r
pub fn binary_expr(l: Expr, op: Operator, r: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(l),
        op,
        right: Box::new(r),
    }
}

/// Trait for converting a type to a [`Literal`] literal expression.
pub trait Literal {
    /// convert the value to a Literal expression
    fn lit(&self) -> Expr;
}

impl Literal for &str {
    fn lit(&self) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some((*self).to_owned())))
    }
}

impl Literal for String {
    fn lit(&self) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some((*self).to_owned())))
    }
}

impl Literal for Vec<u8> {
    fn lit(&self) -> Expr {
        Expr::Literal(ScalarValue::Binary(Some((*self).to_owned())))
    }
}

impl Literal for &[u8] {
    fn lit(&self) -> Expr {
        Expr::Literal(ScalarValue::Binary(Some((*self).to_owned())))
    }
}

impl Literal for ScalarValue {
    fn lit(&self) -> Expr {
        Expr::Literal(self.clone())
    }
}

macro_rules! make_literal {
    ($TYPE:ty, $SCALAR:ident, $DOC: expr) => {
        #[doc = $DOC]
        impl Literal for $TYPE {
            fn lit(&self) -> Expr {
                Expr::Literal(ScalarValue::$SCALAR(Some(self.clone())))
            }
        }
    };
}

make_literal!(bool, Boolean, "literal expression containing a bool");
make_literal!(f32, Float32, "literal expression containing an f32");
make_literal!(f64, Float64, "literal expression containing an f64");
make_literal!(i8, Int8, "literal expression containing an i8");
make_literal!(i16, Int16, "literal expression containing an i16");
make_literal!(i32, Int32, "literal expression containing an i32");
make_literal!(i64, Int64, "literal expression containing an i64");
make_literal!(u8, UInt8, "literal expression containing a u8");
make_literal!(u16, UInt16, "literal expression containing a u16");
make_literal!(u32, UInt32, "literal expression containing a u32");
make_literal!(u64, UInt64, "literal expression containing a u64");

/// Create a literal expression
pub fn lit<T: Literal>(n: T) -> Expr {
    n.lit()
}
