use std::collections::HashMap;
use std::ops::Index;

use daggy::{Dag, EdgeIndex, NodeIndex};

use crate::api::operator::{StreamOperatorWrap, TStreamOperator};
use crate::dag::{DagError, StreamEdge, StreamNode};

#[derive(Debug)]
pub(crate) struct StreamGraph {
    job_name: String,

    stream_nodes: Vec<NodeIndex>,
    stream_edges: Vec<EdgeIndex>,
    // pipeline_operators: HashMap<u32, Vec<StreamOperatorWrap>>,
    operators: HashMap<u32, (NodeIndex, StreamOperatorWrap)>,

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
            // pipeline_operators: HashMap::new(),
            operators: HashMap::new(),
            sources: Vec::new(),
            sinks: Vec::new(),
            dag: Dag::new(),
        }
    }

    pub fn get_dag(&self) -> &Dag<StreamNode, StreamEdge> {
        &self.dag
    }

    pub fn add_operator(&mut self, operator: StreamOperatorWrap) -> Result<(), DagError> {
        let operator_id = operator.get_operator_id();
        let parent_operator_ids = operator.get_parent_operator_ids();

        let stream_node = StreamNode {
            id: operator_id,
            parallelism: operator.get_parallelism(),
            operator_name: operator.get_operator_name().to_string(),
            fn_creator: operator.get_fn_creator(),
        };

        let node_index = self.dag.add_node(stream_node.clone());

        for operator_parent_id in parent_operator_ids {
            let (p_node_index, _operator) = self
                .operators
                .get(&operator_parent_id)
                .ok_or(DagError::ParentOperatorNotFound)?;

            let p_stream_node: &StreamNode = self.dag.index(*p_node_index);

            let stream_edge = StreamEdge {
                edge_id: format!("{}->{}", p_stream_node.id, stream_node.id),
                source_id: p_stream_node.id,
                target_id: stream_node.id,
            };

            let edge_index = self
                .dag
                .add_edge(*p_node_index, node_index, stream_edge.clone())
                .unwrap();

            self.stream_edges.push(edge_index);
        }

        if operator.is_source() {
            self.sources.push(node_index);
        } else if operator.is_sink() {
            self.sinks.push(node_index);
        }
        self.stream_nodes.push(node_index);
        self.operators.insert(operator_id, (node_index, operator));

        Ok(())
    }
}
