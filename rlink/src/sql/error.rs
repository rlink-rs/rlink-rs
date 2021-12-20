//! DataFusion error types

use std::error;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;

use sqlparser::parser::ParserError;

/// Result type for operations that could result in an [DataFusionError]
pub type Result<T> = result::Result<T, DataFusionError>;

/// DataFusion error
#[derive(Debug)]
#[allow(missing_docs)]
pub enum DataFusionError {
    /// Error associated to I/O operations and associated traits.
    IoError(io::Error),
    /// Error returned when SQL is syntactically incorrect.
    SQL(ParserError),
    /// Error returned on a branch that we know it is possible
    /// but to which we still have no implementation for.
    /// Often, these errors are tracked in our issue tracker.
    NotImplemented(String),
    /// Error returned as a consequence of an error in DataFusion.
    /// This error should not happen in normal usage of DataFusion.
    // DataFusions has internal invariants that we are unable to ask the compiler to check for us.
    // This error is raised when one of those invariants is not verified during execution.
    Internal(String),
    /// This error happens whenever a plan is not valid. Examples include
    /// impossible casts, schema inference not possible and non-unique column names.
    Plan(String),
    /// Error returned during execution of the query.
    /// Examples include files not found, errors in parsing certain types.
    Execution(String),
}

impl From<io::Error> for DataFusionError {
    fn from(e: io::Error) -> Self {
        DataFusionError::IoError(e)
    }
}

impl From<ParserError> for DataFusionError {
    fn from(e: ParserError) -> Self {
        DataFusionError::SQL(e)
    }
}

impl Display for DataFusionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match *self {
            DataFusionError::IoError(ref desc) => write!(f, "IO error: {}", desc),
            DataFusionError::SQL(ref desc) => {
                write!(f, "SQL error: {:?}", desc)
            }
            DataFusionError::NotImplemented(ref desc) => {
                write!(f, "This feature is not implemented: {}", desc)
            }
            DataFusionError::Internal(ref desc) => {
                write!(
                    f,
                    "Internal error: {}. This was likely caused by a bug in DataFusion's \
                    code and we would welcome that you file an bug report in our issue tracker",
                    desc
                )
            }
            DataFusionError::Plan(ref desc) => {
                write!(f, "Error during planning: {}", desc)
            }
            DataFusionError::Execution(ref desc) => {
                write!(f, "Execution error: {}", desc)
            }
        }
    }
}

impl error::Error for DataFusionError {}
