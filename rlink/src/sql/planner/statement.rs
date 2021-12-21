use std::sync::Arc;

use sqlparser::ast::{
    BinaryOperator, Expr as SQLExpr, Query, SetExpr, Statement, TableFactor, TableWithJoins, Value,
};
use sqlparser::ast::{DataType as SQLDataType, Select};

use crate::core::data_types::{DataType, Schema};
use crate::sql::error::DataFusionError;
use crate::sql::logical_plain::expr::{lit, Column, Expr};
use crate::sql::logical_plain::operators::Operator;
use crate::sql::logical_plain::plan::Filter;
use crate::sql::logical_plain::plan::LogicalPlan;
use crate::sql::logical_plain::scalar::ScalarValue;
use crate::sql::planner::RegisterTables;

pub struct StatementPlanner<'a, 'b> {
    sql_statement: &'a Statement,
    register_tables: &'b RegisterTables,
}

impl<'a, 'b> StatementPlanner<'a, 'b> {
    pub fn new(sql_statement: &'a Statement, register_tables: &'b RegisterTables) -> Self {
        StatementPlanner {
            sql_statement,
            register_tables,
        }
    }

    /// Generate a logical plan from an SQL statement
    pub fn plan(&self) -> crate::sql::error::Result<LogicalPlan> {
        match self.sql_statement {
            Statement::Query(query) => QueryPlanner::new(query, self.register_tables).plan(),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL statement: {:?}",
                self.sql_statement
            ))),
        }
    }
}

struct QueryPlanner<'a, 'b> {
    query: &'a Query,
    register_tables: &'b RegisterTables,
}

impl<'a, 'b> QueryPlanner<'a, 'b> {
    pub fn new(query: &'a Query, register_tables: &'b RegisterTables) -> Self {
        QueryPlanner {
            query,
            register_tables,
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
            SetExpr::Select(s) => QuerySelectPlanner::new(s, self.register_tables).plan(alias),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Query {} not implemented yet",
                set_expr
            ))),
        }
    }
}

struct QuerySelectPlanner<'a, 'b> {
    select: &'a Select,
    register_tables: &'b RegisterTables,
}

impl<'a, 'b> QuerySelectPlanner<'a, 'b> {
    pub fn new(select: &'a Select, register_tables: &'b RegisterTables) -> Self {
        QuerySelectPlanner {
            select,
            register_tables,
        }
    }

    /// Generate a logic plan from an SQL select
    pub fn plan(&self, alias: Option<String>) -> crate::sql::error::Result<LogicalPlan> {
        // from
        let plan = QuerySelectFromPlanner::new(&self.select.from, self.register_tables).plan()?;

        // selection
        let plan = if let Some(predicate_expr) = &self.select.selection {
            QuerySelectSelectionPlanner::new(predicate_expr, &plan).plan()
        } else {
            Ok(plan)
        }?;

        // group_by
        // let plan = { for n in &select.group_by {} };

        Ok(plan)
    }
}

struct QuerySelectFromPlanner<'a, 'b> {
    from: &'a [TableWithJoins],
    register_tables: &'b RegisterTables,
}

impl<'a, 'b> QuerySelectFromPlanner<'a, 'b> {
    pub fn new(from: &'a [TableWithJoins], register_tables: &'b RegisterTables) -> Self {
        QuerySelectFromPlanner {
            from,
            register_tables,
        }
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
                let cte = self
                    .register_tables
                    .get(&table_name)
                    .ok_or(DataFusionError::Plan(format!(
                        "Table or CTE with name '{}' not found",
                        name
                    )))?;

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

struct QuerySelectSelectionPlanner<'a, 'b> {
    selection: &'a SQLExpr,
    from_plan: &'b LogicalPlan,
}

impl<'a, 'b> QuerySelectSelectionPlanner<'a, 'b> {
    pub fn new(selection: &'a SQLExpr, from_plan: &'b LogicalPlan) -> Self {
        QuerySelectSelectionPlanner {
            selection,
            from_plan,
        }
    }

    pub fn plan(&self) -> crate::sql::error::Result<LogicalPlan> {
        let filter_expr = self.sql_to_rex(self.selection, self.from_plan.schema())?;
        Ok(LogicalPlan::Filter(Filter {
            predicate: filter_expr,
            input: Arc::new(self.from_plan.clone()),
        }))
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_rex(&self, sql: &SQLExpr, schema: &Schema) -> crate::sql::error::Result<Expr> {
        let expr = self.sql_expr_to_logical_expr(sql, schema)?;
        Ok(expr)
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
                let name = if function.name.0.len() > 1 {
                    // DF doesn't handle compound identifiers
                    // (e.g. "foo.bar") for function names yet
                    function.name.to_string()
                } else {
                    // if there is a quote style, then don't normalize
                    // the name, otherwise normalize to lowercase
                    let ident = &function.name.0[0];
                    match ident.quote_style {
                        Some(_) => ident.value.clone(),
                        None => ident.value.to_ascii_lowercase(),
                    }
                };
                Err(DataFusionError::NotImplemented(format!(
                    "Unsupported ast node {:?} in sqltorel",
                    sql
                )))
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
}

struct QuerySelectGroupByPlanner {}

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
