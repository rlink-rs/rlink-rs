use std::collections::HashMap;
use std::ops::Index;

use daggy::{Dag, EdgeIndex, NodeIndex};

use crate::api::data_stream_v2::{PipelineStream, StreamBuilder};
use crate::api::operator::{FunctionCreator, StreamOperatorWrap, TStreamOperator};
use std::error::Error;

#[derive(Debug)]
pub enum DagError {
    OperatorsNotEnough,
}

impl Error for DagError {}

impl std::fmt::Display for DagError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DagError::OperatorsNotEnough => write!(f, "OperatorsNotEnough"),
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

#[derive(Debug)]
pub(crate) struct StreamGraph {
    job_name: String,

    stream_nodes: Vec<NodeIndex>,
    stream_edges: Vec<EdgeIndex>,
    pipeline_operators: HashMap<u32, Vec<StreamOperatorWrap>>,

    sources: Vec<NodeIndex>,
    sinks: Vec<NodeIndex>,

    dag: Dag<StreamNode, StreamEdge>,
}

impl StreamGraph {
    pub fn new(job_name: String) -> Self {
        StreamGraph {
            job_name,
            stream_nodes: Vec::new(),
            stream_edges: Vec::new(),
            pipeline_operators: HashMap::new(),
            sources: Vec::new(),
            sinks: Vec::new(),
            dag: Dag::new(),
        }
    }

    pub fn add_stream(&mut self, data_stream: StreamBuilder) -> Result<NodeIndex, DagError> {
        let pipeline_operators = data_stream.into_operators();
        if pipeline_operators.len() <= 1 {
            return Err(DagError::OperatorsNotEnough);
        }

        self.pipeline_operators.insert(0, pipeline_operators);

        let pipeline_operators = self.pipeline_operators.get(&0).unwrap();
        let mut parent_node_index = None;
        for i in 0..pipeline_operators.len() {
            let operator = &pipeline_operators[i];

            let stream_node = StreamNode {
                id: operator.get_operator_id(),
                parallelism: operator.get_parallelism(),
                operator_name: operator.get_operator_name().to_string(),
                fn_creator: operator.get_fn_creator(),
            };

            let node_index = match parent_node_index {
                Some(p_node_index) => {
                    let p_stream_node: &StreamNode = self.dag.index(p_node_index);

                    let stream_edge = StreamEdge {
                        edge_id: format!("{}_{}", stream_node.id, p_stream_node.id),
                        source_id: p_stream_node.id,
                        target_id: stream_node.id,
                    };

                    let (edge_index, node_index) =
                        self.dag.add_child(p_node_index, stream_edge, stream_node);

                    self.stream_edges.push(edge_index);

                    node_index
                }
                None => self.dag.add_node(stream_node),
            };

            parent_node_index = Some(node_index);
            self.stream_nodes.push(node_index);

            if operator.is_source() {
                self.sources.push(node_index);
            } else if operator.is_sink() {
                self.sinks.push(node_index);
            }
        }

        let latest_node_index = self.stream_nodes.get(self.stream_nodes.len() - 1).unwrap();
        Ok(latest_node_index.clone())
    }
}
