//! DAG builder
//! stream_graph -> job_graph -> execution_graph

use std::error::Error;

use crate::api::operator::{FunctionCreator, StreamOperatorWrap};

// pub(crate) mod execution_graph;
pub(crate) mod job_graph;
pub(crate) mod stream_graph;
pub(crate) mod virtual_io;

pub(crate) use stream_graph::StreamGraph;

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum OperatorType {
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

impl<'a> From<&'a StreamOperatorWrap> for OperatorType {
    fn from(op: &'a StreamOperatorWrap) -> Self {
        match op {
            StreamOperatorWrap::StreamSource(_) => OperatorType::Source,
            StreamOperatorWrap::StreamMap(_) => OperatorType::Map,
            StreamOperatorWrap::StreamFilter(_) => OperatorType::Filter,
            StreamOperatorWrap::StreamCoProcess(_) => OperatorType::CoProcess,
            StreamOperatorWrap::StreamKeyBy(_) => OperatorType::KeyBy,
            StreamOperatorWrap::StreamReduce(_) => OperatorType::Reduce,
            StreamOperatorWrap::StreamWatermarkAssigner(_) => OperatorType::WatermarkAssigner,
            StreamOperatorWrap::StreamWindowAssigner(_) => OperatorType::WindowAssigner,
            StreamOperatorWrap::StreamSink(_) => OperatorType::Sink,
        }
    }
}

impl std::fmt::Display for OperatorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperatorType::Source => write!(f, "Source"),
            OperatorType::Map => write!(f, "Map"),
            OperatorType::Filter => write!(f, "Filter"),
            OperatorType::CoProcess => write!(f, "CoProcess"),
            OperatorType::KeyBy => write!(f, "KeyBy"),
            OperatorType::Reduce => write!(f, "Reduce"),
            OperatorType::WatermarkAssigner => write!(f, "WatermarkAssigner"),
            OperatorType::WindowAssigner => write!(f, "WindowAssigner"),
            OperatorType::Sink => write!(f, "Sink"),
        }
    }
}

#[derive(Debug)]
pub enum DagError {
    SourceNotFound,
    SinkNotFound,
    NotCombineOperator,
    ChildNodeNotFound(OperatorType),
    ParentOperatorNotFound,
    ParallelismInheritUnsupported(OperatorType),
    ChildNotFoundInPipeline,
    MultiChildrenInPipeline,
}

impl Error for DagError {}

impl std::fmt::Display for DagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DagError::SourceNotFound => write!(f, "SourceNotFound"),
            DagError::SinkNotFound => write!(f, "SinkNotFound"),
            DagError::NotCombineOperator => write!(f, "NotCombineOperator"),
            DagError::ChildNodeNotFound(s) => write!(f, "ChildNodeNotFound({})", s),
            DagError::ParentOperatorNotFound => write!(f, "ParentOperatorNotFound"),
            DagError::ParallelismInheritUnsupported(s) => {
                write!(f, "ParallelismInheritUnsupported({})", s)
            }
            DagError::ChildNotFoundInPipeline => write!(f, "ChildNotFoundInPipeline"),
            DagError::MultiChildrenInPipeline => write!(f, "MultiChildrenInPipeline"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StreamNode {
    id: u32,
    parallelism: u32,

    operator_name: String,
    operator_type: OperatorType,
    fn_creator: FunctionCreator,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StreamEdge {
    edge_id: String,

    source_id: u32,
    target_id: u32,
}
