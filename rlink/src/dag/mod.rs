//! DAG builder
//! stream_graph -> job_graph -> execution_graph

use std::error::Error;

use crate::api::operator::FunctionCreator;

pub(crate) mod execution_graph;
pub(crate) mod job_graph;
pub(crate) mod stream_graph;

pub(crate) use stream_graph::StreamGraph;

pub(crate) enum OperatorType {
    Source,
    Map,
    Filter,
    CoProcess,
    KeyBy,
    Reduce,
    WatermarkAssigner,
    WindowAssigner,
    Sink,
}

#[derive(Debug)]
pub enum DagError {
    ChildNodeNotFound(String),
    ParentOperatorNotFound,
}

impl Error for DagError {}

impl std::fmt::Display for DagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DagError::ChildNodeNotFound(s) => write!(f, "ChildNodeNotFound({})", s),
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
