use std::collections::HashMap;
use std::ops::Index;

use daggy::{Dag, EdgeIndex, NodeIndex};

use crate::api::operator::{StreamOperatorWrap, TStreamOperator};
use crate::dag::{DagError, OperatorType, Parallelism, StreamEdge, StreamNode};

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

    // pub fn index(&self, node_index: NodeIndex) -> StreamNode {
    //     self.dag.index(node_index).clone()
    // }

    pub fn add_operator(&mut self, operator: StreamOperatorWrap) -> Result<(), DagError> {
        let operator_id = operator.get_operator_id();
        let parent_operator_ids = operator.get_parent_operator_ids();

        // let parallelism = Parallelism::new(operator.get_parallelism());
        // if parallelism.value_type == ParValueType::Inherit {
        //     let max_parallelism = parent_operator_ids
        //         .iter()
        //         .map(|p_id| self.operators.get(p_id).unwrap().0)
        //         .map(|p_node_index| self.dag.index(p_node_index))
        //         .map(|p_node_stream| p_node_stream.parallelism)
        //         .max_by_key(|p_parallelism| p_parallelism.value)
        //         .expect("can not inherit parallelism");
        //     *parallelism = *max_parallelism
        // }

        let stream_node = StreamNode {
            id: operator_id,
            parallelism: Parallelism::new(operator.get_parallelism()),
            operator_name: operator.get_operator_name().to_string(),
            operator_type: OperatorType::from(&operator),
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

    // fn parallelism_analyse(&mut self, begin_node_indies: Vec<NodeIndex>) -> Result<(), DagError> {
    //     // parse from sources and break with meeting node
    //     let mut meeting_node_set = HashSet::new();
    //     for begin_node_index in begin_node_indies {
    //         let meeting_node_index = self.pipeline_parse(begin_node_index)?;
    //         meeting_node_set.insert(meeting_node_index);
    //     }
    //
    //     for meeting_node_index in meeting_node_set {
    //         let meeting_parallelism = self.dag.index(meeting_node_index).parallelism;
    //         if *meeting_parallelism == 0 {
    //             // inherited the max(parent parallelism).
    //             // that is meeting node's parallelism == max(parent parallelism)
    //
    //             let parents: Vec<(EdgeIndex, NodeIndex)> = self
    //                 .dag
    //                 .parents(meeting_node_index)
    //                 .iter(&self.dag)
    //                 .collect();
    //
    //             let max_parallelism = parents
    //                 .into_iter()
    //                 .map(|(_edge_index, node_index)| self.dag.index(node_index).parallelism)
    //                 .max_by_key(|p| p.value)
    //                 .unwrap();
    //
    //             self.dag.index_mut(meeting_node_index).parallelism = max_parallelism;
    //         }
    //
    //         // go on after meeting's pipeline parse
    //         self.parallelism_analyse(vec![meeting_node_index])?;
    //     }
    //
    //     Ok(())
    // }
    //
    // /// check one node's children in a pipeline stream(children check only)
    // /// the [`node_index`] must have only one parent
    // /// = 0 child: stream finish
    // /// = 1 child: pipeline stream, and go on recursion
    // /// > 1 children:
    // fn pipeline_parse(&mut self, node_index: NodeIndex) -> Result<Option<NodeIndex>, DagError> {
    //     let stream_node = self.dag.index(node_index).clone();
    //     let children: Vec<(EdgeIndex, NodeIndex)> =
    //         self.dag.children(node_index).iter(&self.dag).collect();
    //     let operator = {
    //         let n = self.operators.get(&stream_node.id).unwrap();
    //         &n.1
    //     };
    //
    //     if children.len() == 0 {
    //         return if operator.is_sink() {
    //             Ok(None)
    //         } else {
    //             Err(DagError::ChildNodeNotFound(
    //                 stream_node.operator_name.clone(),
    //             ))
    //         };
    //     }
    //
    //     if children.len() == 1 {
    //         let child = children.get(0).unwrap();
    //         let parents: Vec<(EdgeIndex, NodeIndex)> =
    //             self.dag.parents(child.1).iter(&self.dag).collect();
    //
    //         return if parents.len() == 1 {
    //             let child_stream_index = self.dag.index_mut(child.1);
    //             if child_stream_index.parallelism.value_type == ParValueType::Inherit {
    //                 *child_stream_index.parallelism = *stream_node.parallelism;
    //             }
    //             self.pipeline_parse(child.1)
    //         } else {
    //             // cross node and return
    //             Ok(Some(child.1))
    //         };
    //     }
    //
    //     if children.len() > 1 {
    //         for child in children {
    //             let parents: Vec<(EdgeIndex, NodeIndex)> =
    //                 self.dag.parents(child.1).iter(&self.dag).collect();
    //             let child_stream_node = self.dag.index_mut(child.1);
    //             if child_stream_node.parallelism.value_type == ParValueType::Inherit {
    //                 let max_parallelism = parents
    //                     .iter()
    //                     .map(|parent| self.dag.index(parent.1).parallelism)
    //                     .max_by_key(|parallelism| *parallelism)
    //                     .unwrap();
    //                 *child_stream_node.parallelism = *max_parallelism;
    //
    //                 if parents.len() == 0 {}
    //
    //                 let n = if parents.len() == 1 {
    //                     *child_stream_node.parallelism = *stream_node.parallelism;
    //                     self.pipeline_parse(child.1)
    //                 } else {
    //                     if child_stream_node.parallelism.value_type == ParValueType::Inherit {}
    //
    //                     // cross node and return
    //                     Ok(Some(child.1))
    //                 };
    //             }
    //         }
    //     }
    //
    //     unimplemented!()
    // }
}
