//! DAG builder
//! stream_graph -> job_graph -> execution_graph

use std::error::Error;

use crate::api::operator::StreamOperatorWrap;

pub(crate) mod execution_graph;
pub(crate) mod job_graph;
pub(crate) mod stream_graph;

use daggy::{Dag, NodeIndex, Walker};
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
    // SinkNotFound,
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
            // DagError::SinkNotFound => write!(f, "SinkNotFound"),
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

pub(crate) trait Label {
    fn get_label(&self) -> String;
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct JsonNode<N>
where
    N: Serialize,
{
    id: String,
    label: String,
    #[serde(rename = "type")]
    ty: String,
    detail: Option<N>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct JsonEdge<E>
where
    E: Serialize,
{
    source: String,
    target: String,
    label: String,
    detail: Option<E>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct JsonDag<N, E>
where
    N: Clone + Label + Serialize,
    E: Clone + Label + Serialize,
{
    nodes: Vec<JsonNode<N>>,
    edges: Vec<JsonEdge<E>>,
}

impl<N, E> JsonDag<N, E>
where
    N: Clone + Label + Serialize,
    E: Clone + Label + Serialize,
{
    fn get_node_type(dag: &Dag<N, E>, node_index: NodeIndex) -> &str {
        let parent_count = dag.parents(node_index).iter(dag).count();
        if parent_count == 0 {
            "begin"
        } else {
            let child_count = dag.children(node_index).iter(dag).count();
            if child_count == 0 {
                "end"
            } else {
                ""
            }
        }
    }

    fn crate_json_node(dag: &Dag<N, E>, node_index: NodeIndex) -> JsonNode<N> {
        let n = dag.index(node_index);
        let label = n.get_label();
        let id = node_index.index().to_string();
        let ty = JsonDag::get_node_type(dag, node_index);

        JsonNode {
            id,
            label,
            ty: ty.to_string(),
            detail: Some(n.clone()),
        }
    }

    pub(crate) fn dag_json(dag: &Dag<N, E>) -> Self {
        let mut node_map = HashMap::new();
        let mut edges = Vec::new();

        for edge in dag.raw_edges() {
            let source_json_node = JsonDag::crate_json_node(dag, edge.source());
            let target_json_node = JsonDag::crate_json_node(dag, edge.target());

            let json_edge = {
                let label = edge.weight.get_label();
                JsonEdge {
                    source: source_json_node.id.clone(),
                    target: target_json_node.id.clone(),
                    label,
                    detail: Some(edge.weight.clone()),
                }
            };

            node_map.insert(source_json_node.id.clone(), source_json_node);
            node_map.insert(target_json_node.id.clone(), target_json_node);

            edges.push(json_edge);
        }

        let nodes = node_map.into_iter().map(|(_, node)| node).collect();

        JsonDag { nodes, edges }
    }

    pub(crate) fn fill_begin_end_node(mut self) -> Self {
        let begin_node: JsonNode<N> = JsonNode {
            id: "Begin".to_string(),
            label: "Begin".to_string(),
            ty: "begin".to_string(),
            detail: None,
        };
        let end_node: JsonNode<N> = JsonNode {
            id: "End".to_string(),
            label: "End".to_string(),
            ty: "end".to_string(),
            detail: None,
        };
        let begin_edges: Vec<JsonEdge<E>> = self
            .nodes
            .iter_mut()
            .filter(|node| node.ty.eq("begin"))
            .map(|node| {
                node.ty = "".to_string();
                let edge: JsonEdge<E> = JsonEdge {
                    source: begin_node.id.clone(),
                    target: node.id.clone(),
                    label: "".to_string(),
                    detail: None,
                };
                edge
            })
            .collect();
        let end_edges: Vec<JsonEdge<E>> = self
            .nodes
            .iter_mut()
            .filter(|node| node.ty.eq("end"))
            .map(|node| {
                node.ty = "".to_string();
                let edge: JsonEdge<E> = JsonEdge {
                    source: node.id.clone(),
                    target: end_node.id.clone(),
                    label: "".to_string(),
                    detail: None,
                };
                edge
            })
            .collect();
        self.edges.extend_from_slice(begin_edges.as_slice());
        self.edges.extend_from_slice(end_edges.as_slice());
        self.nodes.push(begin_node);
        self.nodes.push(end_node);

        self
    }

    pub(crate) fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap_or("".to_string())
    }
}
