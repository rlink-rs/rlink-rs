//! DAG builder
//! stream_graph -> job_graph -> execution_graph

use std::error::Error;

use crate::api::operator::StreamOperatorWrap;

pub(crate) mod execution_graph;
pub(crate) mod job_graph;
pub(crate) mod stream_graph;

use daggy::{Dag, Walker};
use serde::Serialize;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Index;
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
    // ChildNodeNotFound(OperatorType),
    ParentOperatorNotFound,
    // ParallelismInheritUnsupported(OperatorType),
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
            // DagError::ChildNodeNotFound(s) => write!(f, "ChildNodeNotFound({})", s),
            DagError::ParentOperatorNotFound => write!(f, "ParentOperatorNotFound"),
            // DagError::ParallelismInheritUnsupported(s) => {
            //     write!(f, "ParallelismInheritUnsupported({})", s)
            // }
            DagError::ChildNotFoundInPipeline => write!(f, "ChildNotFoundInPipeline"),
            DagError::MultiChildrenInPipeline => write!(f, "MultiChildrenInPipeline"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct JsonNode {
    id: String,
    label: String,
    #[serde(rename = "type")]
    ty: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct JsonEdge {
    source: String,
    target: String,
    label: String,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct JsonDag {
    nodes: Vec<JsonNode>,
    edges: Vec<JsonEdge>,
}

pub(crate) fn dag_json<N, E>(dag: &Dag<N, E>) -> JsonDag
where
    N: Debug + Serialize,
    E: Debug + Serialize,
{
    let mut node_map = HashMap::new();
    let mut edges = Vec::new();

    for edge in dag.raw_edges() {
        let source_json_node = {
            let source = edge.source();

            let n = dag.index(source);
            let label = format!("{:?}", n);

            let id = source.index().to_string();

            let parent_count = dag.parents(source).iter(dag).count();
            let ty = if parent_count == 0 {
                "begin"
            } else {
                let child_count = dag.children(source).iter(dag).count();
                if child_count == 0 {
                    "end"
                } else {
                    ""
                }
            };

            JsonNode {
                id,
                label,
                ty: ty.to_string(),
            }
        };

        let target_json_node = {
            let target = edge.target();

            let n = dag.index(target);
            let label = format!("{:?}", n);

            let id = target.index().to_string();

            let parent_count = dag.parents(target).iter(dag).count();
            let ty = if parent_count == 0 {
                "begin"
            } else {
                let child_count = dag.children(target).iter(dag).count();
                if child_count == 0 {
                    "end"
                } else {
                    ""
                }
            };

            JsonNode {
                id,
                label,
                ty: ty.to_string(),
            }
        };

        let json_edge = {
            let label = format!("{:?}", edge.weight);
            JsonEdge {
                source: source_json_node.id.clone(),
                target: target_json_node.id.clone(),
                label,
            }
        };

        node_map.insert(source_json_node.id.clone(), source_json_node);
        node_map.insert(target_json_node.id.clone(), target_json_node);

        edges.push(json_edge);
    }

    let nodes = node_map.into_iter().map(|(_, node)| node).collect();

    JsonDag { nodes, edges }
}
