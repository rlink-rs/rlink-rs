//! DAG builder
//! stream_graph -> job_graph -> execution_graph

use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::ops::Index;

use daggy::{Dag, NodeIndex, Walker};
use serde::Serialize;

use crate::api::function::InputSplit;
use crate::api::operator::StreamOperatorWrap;
use crate::api::runtime::TaskId;
use crate::dag::execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionNode};
use crate::dag::job_graph::{JobEdge, JobGraph, JobNode};
use crate::dag::physic_graph::PhysicGraph;
use crate::dag::stream_graph::StreamGraph;

pub(crate) mod execution_graph;
pub(crate) mod job_graph;
pub(crate) mod physic_graph;
pub(crate) mod stream_graph;
pub(crate) mod utils;

pub(crate) use stream_graph::RawStreamGraph;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct TaskInstance {
    pub task_id: TaskId,
    pub input_split: InputSplit,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct WorkerManagerInstance {
    /// build by self, format `format!("task_manager_{}", index)`
    pub worker_manager_id: String,
    /// chain tasks map: <chain_id, Vec<TaskInstance>>
    pub task_instances: Vec<TaskInstance>,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug)]
pub(crate) struct DagManager {
    stream_graph: StreamGraph,
    job_graph: JobGraph,
    execution_graph: ExecutionGraph,
    physic_graph: PhysicGraph,
}

impl DagManager {
    pub fn new(raw_stream_graph: &RawStreamGraph) -> Self {
        let stream_graph = StreamGraph::new(
            raw_stream_graph.sources.clone(),
            raw_stream_graph.dag.clone(),
        );

        let mut job_graph = JobGraph::new();
        job_graph.build(&stream_graph).unwrap();

        let mut execution_graph = ExecutionGraph::new();
        execution_graph
            .build(&job_graph, raw_stream_graph.get_operators().borrow_mut())
            .unwrap();

        let mut physic_graph = PhysicGraph::new();
        physic_graph.build(&execution_graph);

        DagManager {
            stream_graph,
            job_graph,
            execution_graph,
            physic_graph,
        }
    }

    pub fn stream_graph(&self) -> &StreamGraph {
        &self.stream_graph
    }

    pub fn job_graph(&self) -> &JobGraph {
        &self.job_graph
    }

    pub fn execution_graph(&self) -> &ExecutionGraph {
        &self.execution_graph
    }

    pub fn physic_graph(&self) -> &PhysicGraph {
        &self.physic_graph
    }

    #[inline]
    pub fn get_job_node(&self, task_id: &TaskId) -> Option<JobNode> {
        self.job_graph.get_job_node(task_id.job_id)
    }

    #[inline]
    pub(crate) fn get_job_parents(&self, job_id: u32) -> Vec<(JobNode, JobEdge)> {
        self.job_graph.get_parents(job_id).unwrap_or(vec![])
    }

    #[inline]
    pub(crate) fn get_job_children(&self, job_id: u32) -> Vec<(JobNode, JobEdge)> {
        self.job_graph.get_children(job_id).unwrap_or(vec![])
    }

    pub fn get_task_parents(&self, task_id: &TaskId) -> Vec<(ExecutionNode, ExecutionEdge)> {
        self.execution_graph.get_parents(task_id).unwrap_or(vec![])
    }

    pub fn get_task_children(&self, task_id: &TaskId) -> Vec<(ExecutionNode, ExecutionEdge)> {
        self.execution_graph.get_children(task_id).unwrap_or(vec![])
    }
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
