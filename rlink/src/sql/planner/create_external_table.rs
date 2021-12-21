use sqlparser::ast::ColumnDef as SQLColumnDef;
use sqlparser::ast::DataType as SQLDataType;

use crate::core::data_types::{DataType, Field, Schema};
use crate::sql::error::DataFusionError;
use crate::sql::logical_plain::plan::CreateExternalTable as PlanCreateExternalTable;
use crate::sql::logical_plain::plan::LogicalPlan;
use crate::sql::parser::CreateExternalTable;

pub struct CreateExternalTablePlanner<'a> {
    cet_statement: &'a CreateExternalTable,
}

impl<'a> CreateExternalTablePlanner<'a> {
    pub fn new(cet_statement: &'a CreateExternalTable) -> Self {
        CreateExternalTablePlanner { cet_statement }
    }

    /// Generate a logical plan from a CREATE EXTERNAL TABLE statement
    pub fn plan(&self) -> crate::sql::error::Result<LogicalPlan> {
        let CreateExternalTable {
            name,
            columns,
            with_options,
        } = self.cet_statement;

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
}
