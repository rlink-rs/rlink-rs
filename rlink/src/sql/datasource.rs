//! Data source traits

use std::any::Any;

use async_trait::async_trait;

use crate::core::data_types::Schema;

/// Indicates whether and how a filter expression can be handled by a
/// TableProvider for table scans.
#[derive(Debug, Clone, PartialEq)]
pub enum TableProviderFilterPushDown {
    /// The expression cannot be used by the provider.
    Unsupported,
    /// The expression can be used to help minimise the data retrieved,
    /// but the provider cannot guarantee that all returned tuples
    /// satisfy the filter. The Filter plan node containing this expression
    /// will be preserved.
    Inexact,
    /// The provider guarantees that all returned data satisfies this
    /// filter expression. The Filter plan node containing this expression
    /// will be removed.
    Exact,
}

/// Indicates the type of this table for metadata/catalog purposes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TableType {
    /// An ordinary physical table.
    Base,
    /// A non-materialised table that itself uses a query internally to provide data.
    View,
    /// A transient table.
    Temporary,
}

/// Source table
#[async_trait]
pub trait TableProvider: Sync + Send {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get a reference to the schema for this table
    fn schema(&self) -> Schema;

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }
}
