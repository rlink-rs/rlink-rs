use std::collections::HashMap;
use std::sync::Arc;

use sqlparser::ast::{
    BinaryOperator, ColumnDef as SQLColumnDef, Expr as SQLExpr, Query, SetExpr, Statement,
    TableFactor, TableWithJoins, Value,
};
use sqlparser::ast::{DataType as SQLDataType, Select};

use crate::core::data_types::{DataType, Field, Schema};
use crate::sql::catalog::TableReference;
use crate::sql::datasource::TableProvider;
use crate::sql::error::DataFusionError;
use crate::sql::logical_plain::expr::{lit, Column, Expr};
use crate::sql::logical_plain::operators::Operator;
use crate::sql::logical_plain::plan::LogicalPlan;
use crate::sql::logical_plain::plan::{CreateExternalTable as PlanCreateExternalTable, Filter};
use crate::sql::logical_plain::scalar::ScalarValue;
use crate::sql::parser::{CreateExternalTable, Statement as DFStatement};
use crate::sql::udf::{AggregateUDF, ScalarUDF};

/// The ContextProvider trait allows the query planner to obtain meta-data about tables and
/// functions referenced in SQL statements
pub trait ContextProvider {
    /// Getter for a datasource
    fn get_table_provider(&self, name: TableReference) -> Option<Arc<dyn TableProvider>>;
    /// Getter for a UDF description
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>>;
    /// Getter for a UDAF description
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>>;
}

/// SQL query planner
pub struct SqlToRel<'a, S: ContextProvider> {
    schema_provider: &'a S,
    register_tables: HashMap<String, LogicalPlan>,
}

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Create a new query planner
    pub fn new(schema_provider: &'a S) -> Self {
        SqlToRel {
            schema_provider,
            register_tables: Default::default(),
        }
    }

    /// Generate a logical plan from an DataFusion SQL statement
    pub fn statement_to_plan(
        &mut self,
        statement: &DFStatement,
    ) -> crate::sql::error::Result<LogicalPlan> {
        match statement {
            DFStatement::CreateExternalTable(s) => {
                let lp = self.external_table_to_plan(s)?;
                self.register_tables.insert(s.name.clone(), lp.clone());
                Ok(lp)
            }
            DFStatement::Statement(s) => self.sql_statement_to_plan(s),
        }
    }

    /// Generate a logical plan from a CREATE EXTERNAL TABLE statement
    pub fn external_table_to_plan(
        &self,
        statement: &CreateExternalTable,
    ) -> crate::sql::error::Result<LogicalPlan> {
        let CreateExternalTable {
            name,
            columns,
            with_options,
        } = statement;

        let schema = self.build_schema(columns)?;

        Ok(LogicalPlan::CreateExternalTable(PlanCreateExternalTable {
            schema,
            name: name.clone(),
        }))
    }

    fn build_schema(&self, columns: &[SQLColumnDef]) -> crate::sql::error::Result<Schema> {
        let mut fields = Vec::new();

        for column in columns {
            let data_type = self.make_data_type(&column.data_type)?;
            fields.push(Field::new(&column.name.value, data_type));
        }

        Ok(Schema::new(fields))
    }

    /// Maps the SQL type to the corresponding Arrow `DataType`
    fn make_data_type(&self, sql_type: &SQLDataType) -> crate::sql::error::Result<DataType> {
        match sql_type {
            SQLDataType::BigInt(_) => Ok(DataType::Int64),
            SQLDataType::Int(_) => Ok(DataType::Int32),
            SQLDataType::SmallInt(_) => Ok(DataType::Int16),
            SQLDataType::Char(_) | SQLDataType::Varchar(_) | SQLDataType::Text => {
                Ok(DataType::String)
            }
            SQLDataType::Float(_) => Ok(DataType::Float32),
            SQLDataType::Real => Ok(DataType::Float32),
            SQLDataType::Double => Ok(DataType::Float64),
            SQLDataType::Boolean => Ok(DataType::Boolean),
            _ => Err(DataFusionError::NotImplemented(format!(
                "The SQL data type {:?} is not implemented",
                sql_type
            ))),
        }
    }

    /// Generate a logical plan from an SQL statement
    pub fn sql_statement_to_plan(&self, sql: &Statement) -> crate::sql::error::Result<LogicalPlan> {
        match sql {
            Statement::Query(query) => self.query_to_plan(query),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported SQL statement: {:?}",
                sql
            ))),
        }
    }

    /// Generate a logic plan from an SQL query
    pub fn query_to_plan(&self, query: &Query) -> crate::sql::error::Result<LogicalPlan> {
        self.query_to_plan_with_alias(query, None, &mut HashMap::new())
    }

    /// Generate a logic plan from an SQL query with optional alias
    pub fn query_to_plan_with_alias(
        &self,
        query: &Query,
        alias: Option<String>,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> crate::sql::error::Result<LogicalPlan> {
        if let Some(with) = &query.with {
            return Err(DataFusionError::NotImplemented(with.to_string()));
        }
        if let Some(limit) = &query.limit {
            return Err(DataFusionError::NotImplemented(limit.to_string()));
        }
        if query.order_by.len() > 0 {
            return Err(DataFusionError::NotImplemented("ORDER BY".to_string()));
        }

        let set_expr = &query.body;
        self.set_expr_to_plan(set_expr, alias, ctes)
    }

    fn set_expr_to_plan(
        &self,
        set_expr: &SetExpr,
        alias: Option<String>,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> crate::sql::error::Result<LogicalPlan> {
        match set_expr {
            SetExpr::Select(s) => self.select_to_plan(s.as_ref(), ctes, alias),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Query {} not implemented yet",
                set_expr
            ))),
        }
    }

    fn plan_from_tables(
        &self,
        from: &[TableWithJoins],
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> crate::sql::error::Result<Vec<LogicalPlan>> {
        match from.len() {
            0 => Ok(vec![]),
            _ => from
                .iter()
                .map(|t| self.plan_table_with_joins(t, ctes))
                .collect::<crate::sql::error::Result<Vec<_>>>(),
        }
    }

    fn plan_table_with_joins(
        &self,
        t: &TableWithJoins,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> crate::sql::error::Result<LogicalPlan> {
        let left = self.create_relation(&t.relation, ctes)?;
        match t.joins.len() {
            0 => Ok(left),
            _n => Err(DataFusionError::NotImplemented("JOIN".to_string())),
        }
    }

    fn create_relation(
        &self,
        relation: &TableFactor,
        ctes: &mut HashMap<String, LogicalPlan>,
    ) -> crate::sql::error::Result<LogicalPlan> {
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

    /// Generate a logic plan from an SQL select
    fn select_to_plan(
        &self,
        select: &Select,
        ctes: &mut HashMap<String, LogicalPlan>,
        alias: Option<String>,
    ) -> crate::sql::error::Result<LogicalPlan> {
        // from
        let plan = {
            let plans = self.plan_from_tables(&select.from, ctes)?;
            if plans.len() != 1 {
                Err(DataFusionError::NotImplemented("Join".to_string()))
            } else {
                Ok(plans[0].clone())
            }
        }?;

        // selection
        let plan = if let Some(predicate_expr) = &select.selection {
            let filter_expr = self.sql_to_rex(predicate_expr, plan.schema())?;
            LogicalPlan::Filter(Filter {
                predicate: filter_expr,
                input: Arc::new(plan.clone()),
            })
        } else {
            plan
        };

        // group_by
        // let plan = { for n in &select.group_by {} };

        Ok(plan)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sqlparser::parser::ParserError;

    use crate::sql::catalog::TableReference;
    use crate::sql::datasource::TableProvider;
    use crate::sql::parser::DFParser;
    use crate::sql::planner::{ContextProvider, SqlToRel};
    use crate::sql::udf::{AggregateUDF, ScalarUDF};

    #[test]
    fn create_external_table() -> Result<(), ParserError> {
        // positive case
        let sql = r#"
CREATE EXTERNAL TABLE table_1(a varchar(50), b varchar(50))
    WITH (
    'connector' = 'kafka',
    'topic' = 'a',
    'broker-servers' = '192.168.1.1:9200',
    'startup-mode' = 'earliest-offset',
    'decode-mode' = 'java|json',
    'decode-java-class' = 'x.b.C'
);

SELECT TUMBLE_START(t, INTERVAL '1' minute) as wStart,
       TUMBLE_END(t, INTERVAL '1' minute) as wEnd,
       a, b, myfunc(b), 
       count(*)
FROM table_1 
WHERE a > b AND b < 100 
GROUP BY TUMBLE(t, INTERVAL '1' minute), a, b"#;

        let statements = DFParser::parse_sql(sql)?;
        let mut sql_to_rel = SqlToRel::new(&MockContextProvider);

        for statement in &statements {
            sql_to_rel.statement_to_plan(statement);
        }

        Ok(())
    }

    struct MockContextProvider;

    impl ContextProvider for MockContextProvider {
        fn get_table_provider(&self, name: TableReference) -> Option<Arc<dyn TableProvider>> {
            None
        }

        fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
            None
        }

        fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
            None
        }
    }
}
