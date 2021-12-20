use crate::core::data_types::Schema;
use std::sync::Arc;

use crate::sql::logical_plain::expr::{Column, Expr};

/// Join type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Inner Join
    Inner,
    /// Left Join
    Left,
    /// Right Join
    Right,
    /// Full Join
    Full,
    /// Semi Join
    Semi,
    /// Anti Join
    Anti,
}

/// Evaluates an arbitrary list of expressions (essentially a
/// SELECT with an expression list) on its input.
#[derive(Clone)]
pub struct Projection {
    /// The list of expressions
    pub expr: Vec<Expr>,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The schema description of the output
    pub schema: Schema,
    /// Projection output relation alias
    pub alias: Option<String>,
}

/// Filters rows from its input that do not match an
/// expression (essentially a WHERE clause with a predicate
/// expression).
///
/// Semantically, `<predicate>` is evaluated for each row of the input;
/// If the value of `<predicate>` is true, the input row is passed to
/// the output. If the value of `<predicate>` is false, the row is
/// discarded.
#[derive(Clone)]
pub struct Filter {
    /// The predicate expression, which must have Boolean type.
    pub predicate: Expr,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
}

/// Window its input based on a set of window spec and window function (e.g. SUM or RANK)
#[derive(Clone)]
pub struct Window {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// The window function expression
    pub window_expr: Vec<Expr>,
    /// The schema description of the window output
    pub schema: Schema,
}

/// Union multiple inputs
#[derive(Clone)]
pub struct Union {
    /// Inputs to merge
    pub inputs: Vec<LogicalPlan>,
    /// Union schema. Should be the same for all inputs.
    pub schema: Schema,
    /// Union output relation alias
    pub alias: Option<String>,
}

/// Creates an external table.
#[derive(Clone)]
pub struct CreateExternalTable {
    /// The table schema
    pub schema: Schema,
    /// The table name
    pub name: String,
}

/// Produces the first `n` tuples from its input and discards the rest.
#[derive(Clone)]
pub struct Limit {
    /// The limit
    pub n: usize,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
}

/// Aggregates its input based on a set of grouping and aggregate
/// expressions (e.g. SUM).
#[derive(Clone)]
pub struct Aggregate {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Grouping expressions
    pub group_expr: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_expr: Vec<Expr>,
    /// The schema description of the aggregate output
    pub schema: Schema,
}

/// Sorts its input according to a list of sort expressions.
#[derive(Clone)]
pub struct Sort {
    /// The sort expressions
    pub expr: Vec<Expr>,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
}

/// Join two logical plans on one or more join columns
#[derive(Clone)]
pub struct Join {
    /// Left input
    pub left: Arc<LogicalPlan>,
    /// Right input
    pub right: Arc<LogicalPlan>,
    /// Equijoin clause expressed as pairs of (left, right) join columns
    pub on: Vec<(Column, Column)>,
    /// Join type
    pub join_type: JoinType,
    /// The output schema, containing fields from the left and right inputs
    pub schema: Schema,
    /// If null_equals_null is true, null == null else null != null
    pub null_equals_null: bool,
}

/// A LogicalPlan represents the different types of relational
/// operators (such as Projection, Filter, etc) and can be created by
/// the SQL query planner and the DataFrame API.
///
/// A LogicalPlan represents transforming an input relation (table) to
/// an output relation (table) with a (potentially) different
/// schema. A plan represents a dataflow tree where data flows
/// from leaves up to the root to produce the query result.
#[derive(Clone)]
pub enum LogicalPlan {
    /// Evaluates an arbitrary list of expressions (essentially a
    /// SELECT with an expression list) on its input.
    Projection(Projection),
    /// Filters rows from its input that do not match an
    /// expression (essentially a WHERE clause with a predicate
    /// expression).
    ///
    /// Semantically, `<predicate>` is evaluated for each row of the input;
    /// If the value of `<predicate>` is true, the input row is passed to
    /// the output. If the value of `<predicate>` is false, the row is
    /// discarded.
    Filter(Filter),
    /// Window its input based on a set of window spec and window function (e.g. SUM or RANK)
    Window(Window),
    /// Aggregates its input based on a set of grouping and aggregate
    /// expressions (e.g. SUM).
    Aggregate(Aggregate),
    /// Sorts its input according to a list of sort expressions.
    Sort(Sort),
    /// Join two logical plans on one or more join columns
    Join(Join),
    /// Union multiple inputs
    Union(Union),
    /// Produces the first `n` tuples from its input and discards the rest.
    Limit(Limit),
    /// Creates an external table.
    CreateExternalTable(CreateExternalTable),
}

impl LogicalPlan {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &Schema {
        match self {
            LogicalPlan::Projection(Projection { schema, .. }) => schema,
            LogicalPlan::Filter(Filter { input, .. }) => input.schema(),
            LogicalPlan::Window(Window { schema, .. }) => schema,
            LogicalPlan::Aggregate(Aggregate { schema, .. }) => schema,
            LogicalPlan::Sort(Sort { input, .. }) => input.schema(),
            LogicalPlan::Join(Join { schema, .. }) => schema,
            LogicalPlan::Limit(Limit { input, .. }) => input.schema(),
            LogicalPlan::CreateExternalTable(CreateExternalTable { schema, .. }) => schema,
            LogicalPlan::Union(Union { schema, .. }) => schema,
        }
    }
}
