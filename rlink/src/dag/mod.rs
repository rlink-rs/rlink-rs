use std::error::Error;

use crate::api::operator::FunctionCreator;

pub(crate) mod stream_graph;

pub(crate) use stream_graph::StreamGraph;

#[derive(Debug)]
pub enum DagError {
    // OperatorsNotEnough,
    ParentOperatorNotFound,
}

impl Error for DagError {}

impl std::fmt::Display for DagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // DagError::OperatorsNotEnough => write!(f, "OperatorsNotEnough"),
            DagError::ParentOperatorNotFound => write!(f, "ParentOperatorNotFound"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StreamNode {
    id: u32,
    parallelism: u32,

    operator_name: String,

    fn_creator: FunctionCreator,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StreamEdge {
    edge_id: String,

    source_id: u32,
    target_id: u32,
}
