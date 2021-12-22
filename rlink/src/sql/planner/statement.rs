use std::sync::Arc;
use std::time::Duration;

use sqlparser::ast::{
    BinaryOperator, DateTimeField, Expr as SQLExpr, FunctionArg, Ident, Query, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins, Value,
};
use sqlparser::ast::{DataType as SQLDataType, Select};

use crate::core::data_types::{DataType, Field, Schema};
use crate::sql::error::DataFusionError;
use crate::sql::logical_plain::expr::{lit, Column, Expr};
use crate::sql::logical_plain::operators::Operator;
use crate::sql::logical_plain::plan::{Aggregate, Filter, Projection};
use crate::sql::logical_plain::plan::{LogicalPlan, Window};
use crate::sql::logical_plain::scalar::ScalarValue;
use crate::sql::logical_plain::window_frames::BoundedWindow;
use crate::sql::planner::PlanContext;

pub struct StatementPlanner<'a, 'b> {
    sql_statement: &'a Statement,
    plan_context: &'b PlanContext,
}

impl<'a, 'b> StatementPlanner<'a, 'b> {
    pub fn new(sql_statement: &'a Statement, plan_context: &'b PlanContext) -> Self {
        StatementPlanner {
            sql_statement,
            plan_context,
        }
    }

    /// Generate a logical plan from an SQL statement
    pub fn plan(&self) -> crate::sql::error::Result<LogicalPlan> {
        match self.sql_statement {
            Statement::Query(query) => QueryPlanner::new(query, self.plan_context).plan(),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL statement: {:?}",
                self.sql_statement
            ))),
        }
    }
}

struct QueryPlanner<'a, 'b> {
    query: &'a Query,
    plan_context: &'b PlanContext,
}

impl<'a, 'b> QueryPlanner<'a, 'b> {
    pub fn new(query: &'a Query, plan_context: &'b PlanContext) -> Self {
        QueryPlanner {
            query,
            plan_context,
        }
    }

    /// Generate a logic plan from an SQL query
    pub fn plan(&self) -> crate::sql::error::Result<LogicalPlan> {
        self.query_to_plan_with_alias(None)
    }

    /// Generate a logic plan from an SQL query with optional alias
    pub fn query_to_plan_with_alias(
        &self,
        alias: Option<String>,
    ) -> crate::sql::error::Result<LogicalPlan> {
        if let Some(with) = &self.query.with {
            return Err(DataFusionError::NotImplemented(with.to_string()));
        }
        if let Some(limit) = &self.query.limit {
            return Err(DataFusionError::NotImplemented(limit.to_string()));
        }
        if self.query.order_by.len() > 0 {
            return Err(DataFusionError::NotImplemented("ORDER BY".to_string()));
        }

        let set_expr = &self.query.body;
        self.set_expr_to_plan(set_expr, alias)
    }

    fn set_expr_to_plan(
        &self,
        set_expr: &SetExpr,
        alias: Option<String>,
    ) -> crate::sql::error::Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(s) => QuerySelectPlanner::new(s, self.plan_context).plan(alias),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Query {} not implemented yet",
                set_expr
            ))),
        }
    }
}

struct QuerySelectPlanner<'a, 'b> {
    select: &'a Select,
    plan_context: &'b PlanContext,
}

impl<'a, 'b> QuerySelectPlanner<'a, 'b> {
    pub fn new(select: &'a Select, plan_context: &'b PlanContext) -> Self {
        QuerySelectPlanner {
            select,
            plan_context,
        }
    }

    /// Generate a logic plan from an SQL select
    pub fn plan(&self, alias: Option<String>) -> crate::sql::error::Result<LogicalPlan> {
        // from
        let from_plan = QuerySelectFromPlanner::new(&self.select.from, self.plan_context).plan()?;
        let input_schema = from_plan.schema().clone();

        // selection
        let plan = if let Some(predicate_expr) = &self.select.selection {
            QuerySelectSelectionPlanner::new(
                predicate_expr,
                self.plan_context,
                &input_schema,
                from_plan,
            )
            .plan()
        } else {
            Ok(from_plan)
        }?;

        // group_by
        let plan = if self.select.group_by.len() > 0 {
            QuerySelectGroupByPlanner::new(
                &self.select.group_by,
                self.plan_context,
                &input_schema,
                plan,
            )
            .plan()
        } else {
            Ok(plan)
        }?;

        Ok(plan)
    }
}

struct QuerySelectFromPlanner<'a, 'b> {
    from: &'a [TableWithJoins],
    plan_context: &'b PlanContext,
}

impl<'a, 'b> QuerySelectFromPlanner<'a, 'b> {
    pub fn new(from: &'a [TableWithJoins], plan_context: &'b PlanContext) -> Self {
        QuerySelectFromPlanner { from, plan_context }
    }

    pub fn plan(&self) -> crate::sql::error::Result<LogicalPlan> {
        let plans = self.plan_from_tables(&self.from)?;
        if plans.len() != 1 {
            Err(DataFusionError::NotImplemented("Join".to_string()))
        } else {
            Ok(plans[0].clone())
        }
    }

    fn plan_from_tables(
        &self,
        from: &[TableWithJoins],
    ) -> crate::sql::error::Result<Vec<LogicalPlan>> {
        match from.len() {
            0 => Ok(vec![]),
            _ => from
                .iter()
                .map(|t| self.plan_table_with_joins(t))
                .collect::<crate::sql::error::Result<Vec<_>>>(),
        }
    }

    fn plan_table_with_joins(&self, t: &TableWithJoins) -> crate::sql::error::Result<LogicalPlan> {
        let left = self.create_relation(&t.relation)?;
        match t.joins.len() {
            0 => Ok(left),
            _n => Err(DataFusionError::NotImplemented("JOIN".to_string())),
        }
    }

    fn create_relation(&self, relation: &TableFactor) -> crate::sql::error::Result<LogicalPlan> {
        let (plan, alias) = match relation {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let cte = self.plan_context.register_tables.get(&table_name).ok_or(
                    DataFusionError::Plan(format!("Table or CTE with name '{}' not found", name)),
                )?;

                (cte.clone(), alias)
            }
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported ast node {:?} in create_relation",
                    relation
                )));
            }
        };

        if let Some(alias) = alias {
            let columns_alias = alias.clone().columns;
            if columns_alias.is_empty() {
                // sqlparser-rs encodes AS t as an empty list of column alias
                Ok(plan)
            } else {
                return Err(DataFusionError::NotImplemented(
                    "Unsupported source table given as column alias".to_string(),
                ));
            }
        } else {
            Ok(plan)
        }
    }
}

struct QuerySelectSelectionPlanner<'a, 'b, 'c> {
    selection: &'a SQLExpr,
    plan_context: &'b PlanContext,
    schema: &'c Schema,
    input: Arc<LogicalPlan>,
}

impl<'a, 'b, 'c> QuerySelectSelectionPlanner<'a, 'b, 'c> {
    pub fn new(
        selection: &'a SQLExpr,
        plan_context: &'b PlanContext,
        schema: &'c Schema,
        input: LogicalPlan,
    ) -> Self {
        QuerySelectSelectionPlanner {
            selection,
            plan_context,
            schema,
            input: Arc::new(input),
        }
    }

    pub fn plan(&self) -> crate::sql::error::Result<LogicalPlan> {
        let filter_expr = self.sql_to_rex(self.selection, self.schema)?;
        Ok(LogicalPlan::Filter(Filter {
            predicate: filter_expr,
            input: self.input.clone(),
        }))
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(&self, sql: &SQLExpr, schema: &Schema) -> crate::sql::error::Result<Expr> {
        let expr = ExprPlan::new(sql, self.plan_context, schema).plan()?;
        Ok(expr)
    }
}

struct QuerySelectProjectionPlanner<'a, 'b, 'c> {
    projection: &'a [SelectItem],
    plan_context: &'b PlanContext,
    schema: &'c Schema,
}

impl<'a, 'b, 'c> QuerySelectProjectionPlanner<'a, 'b, 'c> {
    pub fn new(
        projection: &'a [SelectItem],
        plan_context: &'b PlanContext,
        schema: &'c Schema,
    ) -> Self {
        QuerySelectProjectionPlanner {
            projection,
            plan_context,
            schema,
        }
    }

    pub fn pre_plan(&self) -> crate::sql::error::Result<Vec<Expr>> {
        let mut exprs = Vec::new();
        for select_item in self.projection {
            exprs.push(self.sql_select_to_rex(select_item)?);
        }

        Ok(exprs)
    }

    pub fn plan(
        &self,
        expr: Vec<Expr>,
        input: LogicalPlan,
    ) -> crate::sql::error::Result<LogicalPlan> {
        Ok(LogicalPlan::Projection(Projection {
            expr,
            input: Arc::new(input),
            schema: self.schema.clone(),
            alias: None,
        }))
    }

    /// Generate a relational expression from a select SQL expression
    fn sql_select_to_rex(&self, sql: &SelectItem) -> crate::sql::error::Result<Expr> {
        match sql {
            SelectItem::UnnamedExpr(expr) => {
                ExprPlan::new(expr, self.plan_context, self.schema).plan()
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = ExprPlan::new(expr, self.plan_context, self.schema).plan()?;
                Ok(Expr::Alias(Box::new(expr), alias.value.clone()))
            }
            SelectItem::Wildcard => Ok(Expr::Wildcard),
            SelectItem::QualifiedWildcard(_) => Err(DataFusionError::NotImplemented(
                "Qualified wildcards are not supported".to_string(),
            )),
        }
    }
}

struct QuerySelectGroupByPlanner<'a, 'b, 'c> {
    group_by: &'a [SQLExpr],
    plan_context: &'b PlanContext,
    schema: &'c Schema,
    input: Arc<LogicalPlan>,
}

impl<'a, 'b, 'c> QuerySelectGroupByPlanner<'a, 'b, 'c> {
    pub fn new(
        group_by: &'a [SQLExpr],
        plan_context: &'b PlanContext,
        schema: &'c Schema,
        input: LogicalPlan,
    ) -> Self {
        QuerySelectGroupByPlanner {
            group_by,
            plan_context,
            schema,
            input: Arc::new(input),
        }
    }

    pub fn plan(&self) -> crate::sql::error::Result<LogicalPlan> {
        let mut group_exprs = Vec::new();
        let mut aggregate_exprs = Vec::new();
        let mut window_expr = None;
        let mut fields = Vec::new();
        for sql_expr in self.group_by {
            let expr = ExprPlan::new(sql_expr, self.plan_context, self.schema).plan()?;

            if let Expr::WindowFunction { .. } = &expr {
                window_expr = Some(expr);
            } else if let Expr::AggregateFunction { .. } = &expr {
                aggregate_exprs.push(expr);
                fields.push(Field::new("aggregate", DataType::Int64));
            } else if let Expr::ScalarFunction { .. } = &expr {
                group_exprs.push(expr);
                fields.push(Field::new("scalar", DataType::Int64));
            } else if let Expr::Column(column) = &expr {
                let field = self.schema.field_with_name(column.name.as_str()).ok_or(
                    DataFusionError::Plan(format!("column [{}] not found", column.name.as_str())),
                )?;

                group_exprs.push(expr);
                fields.push(field.clone());
            }
        }

        let plan = if window_expr.is_some() {
            Arc::new(LogicalPlan::Window(Window {
                input: self.input.clone(),
                window_expr: vec![window_expr.unwrap()],
                schema: self.schema.clone(),
            }))
        } else {
            self.input.clone()
        };

        Ok(LogicalPlan::Aggregate(Aggregate {
            input: plan,
            group_expr: group_exprs,
            aggr_expr: aggregate_exprs,
            schema: Schema::new(fields),
        }))
    }
}

fn parse_sql_number(n: &str) -> crate::sql::error::Result<Expr> {
    match n.parse::<i64>() {
        Ok(n) => Ok(lit(n)),
        Err(_) => Ok(lit(n.parse::<f64>().unwrap())),
    }
}

/// Convert SQL data type to relational representation of data type
pub fn convert_data_type(sql_type: &SQLDataType) -> crate::sql::error::Result<DataType> {
    match sql_type {
        SQLDataType::Boolean => Ok(DataType::Boolean),
        SQLDataType::SmallInt(_) => Ok(DataType::Int16),
        SQLDataType::Int(_) => Ok(DataType::Int32),
        SQLDataType::BigInt(_) => Ok(DataType::Int64),
        SQLDataType::Float(_) => Ok(DataType::Float32),
        SQLDataType::Real => Ok(DataType::Float32),
        SQLDataType::Double => Ok(DataType::Float64),
        SQLDataType::Char(_) | SQLDataType::Varchar(_) => Ok(DataType::String),
        other => Err(DataFusionError::NotImplemented(format!(
            "Unsupported SQL type {:?}",
            other
        ))),
    }
}

pub struct ExprPlan<'a, 'b, 'c> {
    expr: &'a SQLExpr,
    plan_context: &'b PlanContext,
    schema: &'c Schema,
}

impl<'a, 'b, 'c> ExprPlan<'a, 'b, 'c> {
    pub fn new(expr: &'a SQLExpr, plan_context: &'b PlanContext, schema: &'c Schema) -> Self {
        ExprPlan {
            expr,
            plan_context,
            schema,
        }
    }

    pub fn plan(&self) -> crate::sql::error::Result<Expr> {
        self.sql_expr_to_logical_expr(self.expr, self.schema)
    }

    fn sql_expr_to_logical_expr(
        &self,
        sql: &SQLExpr,
        schema: &Schema,
    ) -> crate::sql::error::Result<Expr> {
        match sql {
            SQLExpr::Value(Value::Number(n, _)) => parse_sql_number(n),
            SQLExpr::Value(Value::SingleQuotedString(ref s)) => Ok(lit(s.clone())),
            SQLExpr::Value(Value::Boolean(n)) => Ok(lit(*n)),
            SQLExpr::Value(Value::Null) => Ok(Expr::Literal(ScalarValue::Utf8(None))),
            SQLExpr::Identifier(ref id) => {
                if id.value.starts_with('@') {
                    Err(DataFusionError::NotImplemented(format!(
                        "ScalarVariable {}",
                        id.value
                    )))
                } else {
                    Ok(Expr::Column(Column {
                        relation: None,
                        name: id.value.clone(),
                    }))
                }
            }
            SQLExpr::CompoundIdentifier(ref ids) => {
                let relation = ids[0].value.clone();
                let name = ids[1].value.clone();
                Ok(Expr::Column(Column {
                    relation: Some(relation),
                    name,
                }))
            }
            SQLExpr::Wildcard => Ok(Expr::Wildcard),
            SQLExpr::Cast {
                ref expr,
                ref data_type,
            } => Ok(Expr::Cast {
                expr: Box::new(self.sql_expr_to_logical_expr(expr, schema)?),
                data_type: convert_data_type(data_type)?,
            }),
            SQLExpr::IsNull(ref expr) => Ok(Expr::IsNull(Box::new(
                self.sql_expr_to_logical_expr(expr, schema)?,
            ))),

            SQLExpr::IsNotNull(ref expr) => Ok(Expr::IsNotNull(Box::new(
                self.sql_expr_to_logical_expr(expr, schema)?,
            ))),

            SQLExpr::IsDistinctFrom(left, right) => Ok(Expr::BinaryExpr {
                left: Box::new(self.sql_expr_to_logical_expr(left, schema)?),
                op: Operator::IsDistinctFrom,
                right: Box::new(self.sql_expr_to_logical_expr(right, schema)?),
            }),

            SQLExpr::IsNotDistinctFrom(left, right) => Ok(Expr::BinaryExpr {
                left: Box::new(self.sql_expr_to_logical_expr(left, schema)?),
                op: Operator::IsNotDistinctFrom,
                right: Box::new(self.sql_expr_to_logical_expr(right, schema)?),
            }),
            SQLExpr::Between {
                ref expr,
                ref negated,
                ref low,
                ref high,
            } => Ok(Expr::Between {
                expr: Box::new(self.sql_expr_to_logical_expr(expr, schema)?),
                negated: *negated,
                low: Box::new(self.sql_expr_to_logical_expr(low, schema)?),
                high: Box::new(self.sql_expr_to_logical_expr(high, schema)?),
            }),

            SQLExpr::InList {
                ref expr,
                ref list,
                ref negated,
            } => {
                let list_expr = list
                    .iter()
                    .map(|e| self.sql_expr_to_logical_expr(e, schema))
                    .collect::<crate::sql::error::Result<Vec<_>>>()?;

                Ok(Expr::InList {
                    expr: Box::new(self.sql_expr_to_logical_expr(expr, schema)?),
                    list: list_expr,
                    negated: *negated,
                })
            }
            SQLExpr::BinaryOp {
                ref left,
                ref op,
                ref right,
            } => self.parse_sql_binary_op(left, op, right, schema),
            SQLExpr::Function(function) => {
                if function.name.0.len() > 1 {
                    // DF doesn't handle compound identifiers
                    // (e.g. "foo.bar") for function names yet
                    Err(DataFusionError::NotImplemented(format!(
                        "Unsupported function with compound identifiers(eg:'foo.bar'), expr: {}",
                        sql
                    )))
                } else {
                    // if there is a quote style, then don't normalize
                    // the name, otherwise normalize to lowercase
                    let ident: &Ident = &function.name.0[0];
                    let name: String = match ident.quote_style {
                        Some(_) => ident.value.clone(),
                        None => ident.value.to_ascii_lowercase(),
                    };
                    self.window_function_plan(name.as_str(), &function.args)
                }
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported ast node {:?} in sqltorel",
                sql
            ))),
        }
    }

    fn parse_sql_binary_op(
        &self,
        left: &SQLExpr,
        op: &BinaryOperator,
        right: &SQLExpr,
        schema: &Schema,
    ) -> crate::sql::error::Result<Expr> {
        let operator = match *op {
            BinaryOperator::Gt => Ok(Operator::Gt),
            BinaryOperator::GtEq => Ok(Operator::GtEq),
            BinaryOperator::Lt => Ok(Operator::Lt),
            BinaryOperator::LtEq => Ok(Operator::LtEq),
            BinaryOperator::Eq => Ok(Operator::Eq),
            BinaryOperator::NotEq => Ok(Operator::NotEq),
            BinaryOperator::Plus => Ok(Operator::Plus),
            BinaryOperator::Minus => Ok(Operator::Minus),
            BinaryOperator::Multiply => Ok(Operator::Multiply),
            BinaryOperator::Divide => Ok(Operator::Divide),
            BinaryOperator::Modulo => Ok(Operator::Modulo),
            BinaryOperator::And => Ok(Operator::And),
            BinaryOperator::Or => Ok(Operator::Or),
            BinaryOperator::Like => Ok(Operator::Like),
            BinaryOperator::NotLike => Ok(Operator::NotLike),
            BinaryOperator::PGRegexMatch => Ok(Operator::RegexMatch),
            BinaryOperator::PGRegexIMatch => Ok(Operator::RegexIMatch),
            BinaryOperator::PGRegexNotMatch => Ok(Operator::RegexNotMatch),
            BinaryOperator::PGRegexNotIMatch => Ok(Operator::RegexNotIMatch),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL binary operator {:?}",
                op
            ))),
        }?;

        Ok(Expr::BinaryExpr {
            left: Box::new(self.sql_expr_to_logical_expr(left, schema)?),
            op: operator,
            right: Box::new(self.sql_expr_to_logical_expr(right, schema)?),
        })
    }

    fn window_function_plan(
        &self,
        name: &str,
        args: &[FunctionArg],
    ) -> crate::sql::error::Result<Expr> {
        match name {
            "hop" => {
                if args.len() != 3 {
                    return Err(DataFusionError::Plan(
                        "HOP function with 2 arguments".to_string(),
                    ));
                }

                let time_column = self.function_arg_to_string(&args[0])?;
                let size = self.function_arg_to_duration(&args[1])?;
                let side = self.function_arg_to_duration(&args[2])?;

                Ok(Expr::WindowFunction {
                    args: vec![Expr::Column(Column {
                        relation: None,
                        name: time_column,
                    })],
                    window_frame: BoundedWindow::HOP { size, side },
                })
            }
            "tumble" => {
                if args.len() != 2 && args.len() != 3 {
                    return Err(DataFusionError::Plan(
                        "Tumble function with 1 or 2 arguments".to_string(),
                    ));
                }

                let time_column = self.function_arg_to_string(&args[0])?;
                let size = self.function_arg_to_duration(&args[1])?;
                let offset = if args.len() == 3 {
                    Some(self.function_arg_to_duration(&args[2])?)
                } else {
                    None
                };

                Ok(Expr::WindowFunction {
                    args: vec![Expr::Column(Column {
                        relation: None,
                        name: time_column,
                    })],
                    window_frame: BoundedWindow::Tumble { size, offset },
                })
            }
            _ => Err(DataFusionError::Plan(format!(
                "Unsupported function {}",
                name
            ))),
        }
    }

    fn function_arg_to_duration(
        &self,
        function_arg: &FunctionArg,
    ) -> crate::sql::error::Result<Duration> {
        match function_arg {
            FunctionArg::Unnamed(expr) => {
                let value = expr_as_value(expr)?;
                value_to_interval(value)
            }
            _ => Err(DataFusionError::Plan(format!(
                "Unsupported plan {} in window function with 2 arguments",
                function_arg
            ))),
        }
    }

    fn function_arg_to_string(
        &self,
        function_arg: &FunctionArg,
    ) -> crate::sql::error::Result<String> {
        match function_arg {
            FunctionArg::Unnamed(expr) => {
                let ident = expr_as_identifier(expr)?;
                Ok(ident.value.clone())
            }
            _ => Err(DataFusionError::Plan(format!(
                "Unsupported plan {} in window function with 2 arguments",
                function_arg
            ))),
        }
    }
}

fn expr_as_identifier(expr: &SQLExpr) -> crate::sql::error::Result<&Ident> {
    match expr {
        SQLExpr::Identifier(id) => Ok(id),
        _ => Err(DataFusionError::Plan(format!("Unsupported {}", expr))),
    }
}

fn expr_as_value(expr: &SQLExpr) -> crate::sql::error::Result<&Value> {
    match expr {
        SQLExpr::Value(value) => Ok(value),
        _ => Err(DataFusionError::Plan(format!("Unsupported {}", expr))),
    }
}

fn value_to_interval(v: &Value) -> crate::sql::error::Result<Duration> {
    match v {
        Value::Interval {
            value,
            leading_field,
            ..
        } => {
            let value_part: usize = value.parse::<usize>().map_err(|e| {
                DataFusionError::Plan(format!("parse Interval error {}, expr: {}", e, value))
            })?;

            let leading_field = leading_field.as_ref().ok_or(DataFusionError::Plan(format!(
                "parse Interval error, expr: {}",
                value
            )))?;

            match leading_field {
                DateTimeField::Day => Ok(Duration::from_secs((value_part * 24 * 60 * 60) as u64)),
                DateTimeField::Hour => Ok(Duration::from_secs((value_part * 60 * 60) as u64)),
                DateTimeField::Minute => Ok(Duration::from_secs((value_part * 60) as u64)),
                DateTimeField::Second => Ok(Duration::from_secs((value_part) as u64)),
                _ => Err(DataFusionError::Plan(format!(
                    "Unsupported DateTimeField {}",
                    leading_field
                ))),
            }
        }
        _ => Err(DataFusionError::Plan(format!("Unsupported {:?}", v))),
    }
}
