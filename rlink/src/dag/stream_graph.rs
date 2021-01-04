use std::collections::HashMap;
use std::ops::Index;

use daggy::{Dag, EdgeIndex, NodeIndex};

use crate::api::operator::{
    FunctionCreator, StreamOperator, StreamOperatorWrap, TStreamOperator, DEFAULT_PARALLELISM,
};
use crate::dag::virtual_io::{VirtualInputFormat, VirtualOutputFormat};
use crate::dag::{DagError, OperatorType, StreamEdge, StreamNode};
use std::cmp::max;

pub type OperatorId = u32;

#[derive(Debug)]
pub(crate) struct StreamGraph {
    job_name: String,

    stream_nodes: Vec<NodeIndex>,
    stream_edges: Vec<EdgeIndex>,

    id_gen: OperatorId,
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
            id_gen: 0,
            operators: HashMap::new(),
            sources: Vec::new(),
            sinks: Vec::new(),
            dag: Dag::new(),
        }
    }

    pub fn get_dag(&self) -> &Dag<StreamNode, StreamEdge> {
        &self.dag
    }

    fn create_virtual_source(
        &mut self,
        id: u32,
        parent_id: u32,
        parallelism: u32,
    ) -> StreamOperatorWrap {
        let input_format = Box::new(VirtualInputFormat {});
        StreamOperatorWrap::StreamSource(StreamOperator::new(
            id,
            vec![parent_id],
            parallelism,
            FunctionCreator::System,
            input_format,
        ))
    }

    fn create_virtual_sink(
        &mut self,
        id: u32,
        parent_id: OperatorId,
        parallelism: u32,
    ) -> StreamOperatorWrap {
        let output_format = Box::new(VirtualOutputFormat {});
        StreamOperatorWrap::StreamSink(StreamOperator::new(
            id,
            vec![parent_id],
            parallelism,
            FunctionCreator::System,
            output_format,
        ))
    }

    fn add_operator0(
        &mut self,
        operator: StreamOperatorWrap,
        parent_operator_ids: Vec<OperatorId>,
        parallelism: u32,
    ) -> Result<OperatorId, DagError> {
        let operator_id = self.id_gen;
        self.id_gen += 1;

        let stream_node = StreamNode {
            id: operator_id,
            parallelism,
            operator_name: operator.get_operator_name().to_string(),
            operator_type: OperatorType::from(&operator),
            fn_creator: operator.get_fn_creator(),
        };

        let node_index = self.dag.add_node(stream_node.clone());

        for operator_parent_id in &parent_operator_ids {
            let (p_node_index, _operator) = self
                .operators
                .get(operator_parent_id)
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

        if operator.is_source() && parent_operator_ids.len() == 0 {
            self.sources.push(node_index);
        }
        self.operators.insert(operator_id, (node_index, operator));

        Ok(operator_id)
    }

    pub fn add_operator(
        &mut self,
        operator: StreamOperatorWrap,
        parent_operator_ids: Vec<OperatorId>,
    ) -> Result<OperatorId, DagError> {
        let parallelism = operator.get_parallelism();
        let operator_type = OperatorType::from(&operator);

        return if parent_operator_ids.len() == 0 {
            self.add_operator0(operator, parent_operator_ids, parallelism)
        } else if parent_operator_ids.len() == 1 {
            let p_operator_id = parent_operator_ids[0];
            let (p_node_index, _) = self.operators.get(&p_operator_id).unwrap();
            let p_stream_node = self.dag.index(*p_node_index);

            let p_parallelism = p_stream_node.parallelism;
            let p_operator_type = p_stream_node.operator_type;

            if self.is_pipeline(operator_type, parallelism, p_operator_type, p_parallelism) {
                // tow types of parallelism inherit
                // 1. Forward:  source->map
                // 2. Backward: window->reduce
                let parallelism = max(parallelism, p_parallelism);
                self.add_operator0(operator, parent_operator_ids, parallelism)
            } else {
                let vir_sink = self.create_virtual_sink(0, p_operator_id, p_parallelism);
                let vir_operator_id =
                    self.add_operator0(vir_sink, vec![p_operator_id], p_parallelism)?;

                let vir_source = self.create_virtual_source(0, vir_operator_id, parallelism);
                let vir_operator_id =
                    self.add_operator0(vir_source, vec![vir_operator_id], parallelism)?;

                self.add_operator0(operator, vec![vir_operator_id], parallelism)
            }
        } else {
            if operator_type != OperatorType::CoProcess {
                return Err(DagError::NotCombineOperator);
            }

            let mut new_p_operator_ids = Vec::new();
            for p_operator_id in parent_operator_ids {
                let (p_node_index, _) = self.operators.get(&p_operator_id).unwrap();
                let p_stream_node = self.dag.index(*p_node_index);

                let p_parallelism = p_stream_node.parallelism;
                let vir_sink = self.create_virtual_sink(0, 0, 0);
                let vir_operator_id =
                    self.add_operator0(vir_sink, vec![p_operator_id], p_parallelism)?;

                new_p_operator_ids.push(vir_operator_id);
            }

            let vir_source = self.create_virtual_source(0, 0, 0);
            let vir_operator_id =
                self.add_operator0(vir_source, new_p_operator_ids, parallelism)?;

            self.add_operator0(operator, vec![vir_operator_id], parallelism)
        };
    }

    fn is_pipeline(
        &self,
        operator_type: OperatorType,
        parallelism: u32,
        p_operator_type: OperatorType,
        p_parallelism: u32,
    ) -> bool {
        if p_operator_type == OperatorType::WindowAssigner && operator_type == OperatorType::Reduce
        {
            true
        } else {
            if self.is_pipeline_op(operator_type, p_operator_type)
                && (parallelism == p_parallelism || parallelism == DEFAULT_PARALLELISM)
            {
                true
            } else {
                false
            }
        }
    }

    fn is_pipeline_op(
        &self,
        operator_type: OperatorType,
        parent_operator_type: OperatorType,
    ) -> bool {
        match parent_operator_type {
            OperatorType::Source => match operator_type {
                OperatorType::Map
                | OperatorType::Filter
                | OperatorType::WatermarkAssigner
                | OperatorType::KeyBy
                | OperatorType::Sink => true,
                OperatorType::Source => panic!("Source is the start Operator"),
                _ => false,
            },
            OperatorType::Map | OperatorType::Filter | OperatorType::WatermarkAssigner => {
                match operator_type {
                    OperatorType::Map
                    | OperatorType::Filter
                    | OperatorType::WatermarkAssigner
                    | OperatorType::KeyBy
                    | OperatorType::Sink => true,
                    OperatorType::Source => panic!("Source is the start Operator"),
                    _ => false,
                }
            }
            OperatorType::CoProcess => match operator_type {
                OperatorType::KeyBy => true,
                OperatorType::Source => panic!("Source is the start Operator"),
                _ => false,
            },
            OperatorType::KeyBy => match operator_type {
                OperatorType::Source => panic!("Source is the start Operator"),
                _ => false,
            },
            OperatorType::WindowAssigner => match operator_type {
                OperatorType::Reduce => true,
                OperatorType::Source => panic!("Source is the start Operator"),
                _ => false,
            },
            OperatorType::Reduce => match operator_type {
                OperatorType::Source => panic!("Source is the start Operator"),
                _ => false,
            },
            OperatorType::Sink => panic!("Sink is end Operator"),
        }
    }
}
