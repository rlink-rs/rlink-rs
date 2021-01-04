//! DAG builder
//! stream_graph -> job_graph -> execution_graph

use std::error::Error;

use crate::api::operator::{FunctionCreator, StreamOperatorWrap};

// pub(crate) mod execution_graph;
// pub(crate) mod job_graph;
pub(crate) mod stream_graph;
pub(crate) mod virtual_io;

use std::borrow::BorrowMut;
use std::ops::{Deref, DerefMut};
pub(crate) use stream_graph::StreamGraph;

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub(crate) enum ParValueType {
    /// by user custom
    Custom,
    /// inherit from parent node
    Inherit,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub(crate) struct Parallelism {
    value: u32,
    value_type: ParValueType,
}

impl Parallelism {
    pub fn new(value: u32) -> Self {
        let value_type = if value == 0 {
            ParValueType::Inherit
        } else {
            ParValueType::Custom
        };
        Parallelism { value, value_type }
    }
}

impl Deref for Parallelism {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for Parallelism {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value.borrow_mut()
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
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
    ChildNodeNotFound(OperatorType),
    ParentOperatorNotFound,
    ParallelismInheritUnsupported(OperatorType),
}

impl Error for DagError {}

impl std::fmt::Display for DagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DagError::SourceNotFound => write!(f, "SourceNotFound"),
            DagError::SinkNotFound => write!(f, "SinkNotFound"),
            DagError::ChildNodeNotFound(s) => write!(f, "ChildNodeNotFound({})", s),
            DagError::ParentOperatorNotFound => write!(f, "ParentOperatorNotFound"),
            DagError::ParallelismInheritUnsupported(s) => {
                write!(f, "ParallelismInheritUnsupported({})", s)
            }
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StreamNode {
    id: u32,
    parallelism: Parallelism,

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
