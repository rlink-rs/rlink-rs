use std::collections::HashMap;
use std::sync::Arc;

use crate::sql::catalog::TableReference;
use crate::sql::datasource::TableProvider;
use crate::sql::logical_plain::plan::LogicalPlan;
use crate::sql::parser::Statement as DFStatement;
use crate::sql::planner::create_external_table::CreateExternalTablePlanner;
use crate::sql::planner::statement::StatementPlanner;
use crate::sql::udf::{AggregateUDF, ScalarUDF};

mod create_external_table;
mod statement;

/// The ContextProvider trait allows the query planner to obtain meta-data about tables and
/// functions referenced in SQL statements
pub trait ContextProvider {
    /// Getter for a datasource
    fn get_table_provider(&self, name: TableReference) -> Option<Arc<dyn TableProvider>>;
    /// Getter for a UDF description
    fn get_scalar_meta(&self, name: &str) -> Option<Arc<ScalarUDF>>;
    /// Getter for a UDAF description
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>>;
}

#[derive(Default)]
pub struct RegisterTables {
    table_logic_plan: HashMap<String, LogicalPlan>,
}

impl RegisterTables {
    pub fn new() -> Self {
        RegisterTables {
            table_logic_plan: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: &str, plan: LogicalPlan) {
        self.table_logic_plan.insert(name.to_string(), plan);
    }

    pub fn get(&self, name: &str) -> Option<&LogicalPlan> {
        self.table_logic_plan.get(name)
    }
}

pub struct PlanContext {
    context_provider: Box<dyn ContextProvider>,
    register_tables: RegisterTables,
}

impl PlanContext {
    pub fn new(context_provider: Box<dyn ContextProvider>) -> Self {
        PlanContext {
            context_provider,
            register_tables: Default::default(),
        }
    }
}

/// SQL query planner
pub struct SqlToRel {
    context: PlanContext,
}

impl SqlToRel {
    /// Create a new query planner
    pub fn new(context: PlanContext) -> Self {
        SqlToRel { context }
    }

    /// Generate a logical plan from an DataFusion SQL statement
    pub fn statement_to_plan(
        &mut self,
        statement: &DFStatement,
    ) -> crate::sql::error::Result<LogicalPlan> {
        match statement {
            DFStatement::CreateExternalTable(s) => {
                let cet_plan = CreateExternalTablePlanner::new(s).plan()?;
                self.context
                    .register_tables
                    .register(s.name.as_str(), cet_plan.clone());
                Ok(cet_plan)
            }
            DFStatement::Statement(s) => StatementPlanner::new(s, &self.context).plan(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sqlparser::parser::ParserError;

    use crate::sql::catalog::TableReference;
    use crate::sql::datasource::TableProvider;
    use crate::sql::parser::DFParser;
    use crate::sql::planner::{ContextProvider, PlanContext, SqlToRel};
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
       tb.a, 
       b, 
       myfunc(b), 
       count(*)
FROM   table_1 as tb
WHERE  tb.a > tb.b AND b < 100 
GROUP BY TUMBLE(t, INTERVAL '1' minute), tb.a, b"#;

        let statements = DFParser::parse_sql(sql)?;
        let mut sql_to_rel = SqlToRel::new(PlanContext::new(Box::new(MockContextProvider)));

        for statement in &statements {
            let plan = sql_to_rel.statement_to_plan(statement).unwrap();
            println!("{:?}", plan.schema());
        }

        Ok(())
    }

    struct MockContextProvider;

    impl ContextProvider for MockContextProvider {
        fn get_table_provider(&self, name: TableReference) -> Option<Arc<dyn TableProvider>> {
            None
        }

        fn get_scalar_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
            None
        }

        fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
            None
        }
    }
}
