use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::sql::datasource::TableProvider;
use crate::sql::error::DataFusionError;

/// Represents a resolved path to a table of the form "catalog.schema.table"
#[derive(Clone, Copy)]
pub struct ResolvedTableReference<'a> {
    /// The catalog (aka database) containing the table
    pub catalog: &'a str,
    /// The schema containing the table
    pub schema: &'a str,
    /// The table name
    pub table: &'a str,
}

/// Represents a path to a table that may require further resolution
#[derive(Clone, Copy)]
pub enum TableReference<'a> {
    /// An unqualified table reference, e.g. "table"
    Bare {
        /// The table name
        table: &'a str,
    },
    /// A partially resolved table reference, e.g. "schema.table"
    Partial {
        /// The schema containing the table
        schema: &'a str,
        /// The table name
        table: &'a str,
    },
    /// A fully resolved table reference, e.g. "catalog.schema.table"
    Full {
        /// The catalog (aka database) containing the table
        catalog: &'a str,
        /// The schema containing the table
        schema: &'a str,
        /// The table name
        table: &'a str,
    },
}

impl<'a> TableReference<'a> {
    /// Retrieve the actual table name, regardless of qualification
    pub fn table(&self) -> &str {
        match self {
            Self::Full { table, .. } | Self::Partial { table, .. } | Self::Bare { table } => table,
        }
    }

    /// Given a default catalog and schema, ensure this table reference is fully resolved
    pub fn resolve(
        self,
        default_catalog: &'a str,
        default_schema: &'a str,
    ) -> ResolvedTableReference<'a> {
        match self {
            Self::Full {
                catalog,
                schema,
                table,
            } => ResolvedTableReference {
                catalog,
                schema,
                table,
            },
            Self::Partial { schema, table } => ResolvedTableReference {
                catalog: default_catalog,
                schema,
                table,
            },
            Self::Bare { table } => ResolvedTableReference {
                catalog: default_catalog,
                schema: default_schema,
                table,
            },
        }
    }
}

impl<'a> From<&'a str> for TableReference<'a> {
    fn from(s: &'a str) -> Self {
        Self::Bare { table: s }
    }
}

impl<'a> From<ResolvedTableReference<'a>> for TableReference<'a> {
    fn from(resolved: ResolvedTableReference<'a>) -> Self {
        Self::Full {
            catalog: resolved.catalog,
            schema: resolved.schema,
            table: resolved.table,
        }
    }
}

impl<'a> TryFrom<&'a sqlparser::ast::ObjectName> for TableReference<'a> {
    type Error = DataFusionError;

    fn try_from(value: &'a sqlparser::ast::ObjectName) -> Result<Self, Self::Error> {
        let idents = &value.0;

        match idents.len() {
            1 => Ok(Self::Bare {
                table: &idents[0].value,
            }),
            2 => Ok(Self::Partial {
                schema: &idents[0].value,
                table: &idents[1].value,
            }),
            3 => Ok(Self::Full {
                catalog: &idents[0].value,
                schema: &idents[1].value,
                table: &idents[2].value,
            }),
            _ => Err(DataFusionError::Plan(format!(
                "invalid table reference: {}",
                value
            ))),
        }
    }
}

/// Represents a schema, comprising a number of named tables.
pub trait SchemaProvider: Sync + Send {
    /// Returns the schema provider as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String>;

    /// Retrieves a specific table from the schema by name, provided it exists.
    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>>;

    /// If supported by the implementation, adds a new table to this schema.
    /// If a table of the same name existed before, it returns "Table already exists" error.
    #[allow(unused_variables)]
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> crate::sql::error::Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::Execution(
            "schema provider does not support registering tables".to_owned(),
        ))
    }

    /// If supported by the implementation, removes an existing table from this schema and returns it.
    /// If no table of that name exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(
        &self,
        name: &str,
    ) -> crate::sql::error::Result<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::Execution(
            "schema provider does not support deregistering tables".to_owned(),
        ))
    }

    /// If supported by the implementation, checks the table exist in the schema provider or not.
    /// If no matched table in the schema provider, return false.
    /// Otherwise, return true.
    fn table_exist(&self, name: &str) -> bool;
}
