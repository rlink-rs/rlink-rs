use std::cmp::max;
use std::collections::HashMap;
use std::ops::Index;

use daggy::{Dag, EdgeIndex, NodeIndex};

use crate::core::element::FnSchema;
use crate::core::operator::{
    DefaultStreamOperator, FunctionCreator, StreamOperator, TStreamOperator, DEFAULT_PARALLELISM,
};
use crate::core::runtime::OperatorId;
use crate::dag::{DagError, OperatorType};
use crate::functions::system::keyed_state_flat_map::KeyedStateFlatMapFunction;
use crate::functions::system::system_input_format::SystemInputFormat;
use crate::functions::system::system_output_format::SystemOutputFormat;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StreamNode {
    pub(crate) id: OperatorId,
    pub(crate) parent_ids: Vec<OperatorId>,
    pub(crate) parallelism: u16,
    pub(crate) input_schema: FnSchema,
    pub(crate) output_schema: FnSchema,
    pub(crate) daemon: bool,

    pub(crate) operator_name: String,
    pub(crate) operator_type: OperatorType,
    pub(crate) fn_creator: FunctionCreator,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct StreamEdge {
    edge_id: String,

    source_id: OperatorId,
    target_id: OperatorId,
}

#[derive(Debug, Clone)]
pub(crate) struct StreamGraph {
    pub(crate) sources: Vec<NodeIndex>,
    pub(crate) dag: Dag<StreamNode, StreamEdge>,
}

impl StreamGraph {
    pub fn new(sources: Vec<NodeIndex>, dag: Dag<StreamNode, StreamEdge>) -> Self {
        StreamGraph { sources, dag }
    }

    pub fn stream_node(&self, node_index: NodeIndex) -> &StreamNode {
        self.dag.index(node_index)
    }
}

#[derive(Debug)]
pub(crate) struct RawStreamGraph {
    // stream_nodes: Vec<NodeIndex>,
    stream_edges: Vec<EdgeIndex>,

    id_gen: OperatorId,
    operators: HashMap<OperatorId, (NodeIndex, StreamOperator)>,

    pub(crate) sources: Vec<NodeIndex>,
    pub(crate) user_sources: Vec<NodeIndex>,

    // sinks: Vec<NodeIndex>,
    pub(crate) dag: Dag<StreamNode, StreamEdge>,
}

impl RawStreamGraph {
    pub fn new() -> Self {
        RawStreamGraph {
            // stream_nodes: Vec::new(),
            stream_edges: Vec::new(),
            id_gen: OperatorId::default(),
            operators: HashMap::new(),
            sources: Vec::new(),
            user_sources: Vec::new(),
            // sinks: Vec::new(),
            dag: Dag::new(),
        }
    }

    pub fn pop_operators(&mut self) -> HashMap<OperatorId, StreamOperator> {
        let operator_ids: Vec<OperatorId> = self.operators.iter().map(|(x, _)| *x).collect();

        let mut operators = HashMap::new();
        operator_ids.iter().for_each(|operator_id| {
            let (_, op) = self.operators.remove(operator_id).unwrap();
            operators.insert(*operator_id, op);
        });

        operators
    }

    pub fn operators(&self) -> HashMap<OperatorId, &StreamOperator> {
        let operator_ids: Vec<OperatorId> = self.operators.iter().map(|(x, _)| *x).collect();

        let mut operators = HashMap::new();
        operator_ids.iter().for_each(|operator_id| {
            let (_, op) = self.operators.get(operator_id).unwrap();
            operators.insert(*operator_id, op);
        });

        operators
    }

    fn create_virtual_flat_map(&mut self, parallelism: u16) -> StreamOperator {
        let map_format = Box::new(KeyedStateFlatMapFunction::new());
        StreamOperator::StreamFlatMap(DefaultStreamOperator::new(
            parallelism,
            FunctionCreator::System,
            map_format,
        ))
    }

    fn create_virtual_source(&mut self, parallelism: u16) -> StreamOperator {
        let input_format = Box::new(SystemInputFormat::new());
        StreamOperator::StreamSource(DefaultStreamOperator::new(
            parallelism,
            FunctionCreator::System,
            input_format,
        ))
    }

    fn create_virtual_sink(&mut self, parallelism: u16) -> StreamOperator {
        let output_format = Box::new(SystemOutputFormat::new());
        StreamOperator::StreamSink(DefaultStreamOperator::new(
            parallelism,
            FunctionCreator::System,
            output_format,
        ))
    }

    fn add_operator0(
        &mut self,
        operator: StreamOperator,
        parent_operator_ids: Vec<OperatorId>,
        parallelism: u16,
    ) -> Result<OperatorId, DagError> {
        let operator_id = self.id_gen;
        self.id_gen.0 = self.id_gen.0 + 1;

        let input_schema = match parent_operator_ids.len() {
            0 => FnSchema::Empty,
            1 => {
                let (p_node_index, _p_operator) =
                    self.operators.get(&parent_operator_ids[0]).unwrap();
                self.dag.index(*p_node_index).output_schema.clone()
            }
            _ => {
                let (p_node_index, _p_operator) = self
                    .operators
                    .get(&parent_operator_ids[parent_operator_ids.len() - 1])
                    .unwrap();
                self.dag.index(*p_node_index).output_schema.clone()
            }
        };

        let stream_node = StreamNode {
            id: operator_id,
            parent_ids: parent_operator_ids.clone(),
            parallelism,
            input_schema: input_schema.clone(),
            output_schema: operator.schema(input_schema),
            daemon: operator.is_daemon(),
            operator_name: operator.operator_name().to_string(),
            operator_type: OperatorType::from(&operator),
            fn_creator: operator.fn_creator(),
        };

        let node_index = self.dag.add_node(stream_node.clone());

        for operator_parent_id in &parent_operator_ids {
            let (p_node_index, _operator) = self
                .operators
                .get(operator_parent_id)
                .ok_or(DagError::ParentOperatorNotFound)?;

            let p_stream_node: &StreamNode = self.dag.index(*p_node_index);

            let stream_edge = StreamEdge {
                edge_id: format!("{:?}->{:?}", p_stream_node.id.0, stream_node.id.0),
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
            if parent_operator_ids.len() == 0 {
                self.user_sources.push(node_index);
            }
            self.sources.push(node_index);
        }
        self.operators.insert(operator_id, (node_index, operator));

        Ok(operator_id)
    }

    pub fn add_operator(
        &mut self,
        operator: StreamOperator,
        parent_operator_ids: Vec<OperatorId>,
    ) -> Result<OperatorId, DagError> {
        let parallelism = operator.parallelism();
        let operator_type = OperatorType::from(&operator);

        return if parent_operator_ids.len() == 0 {
            if operator_type != OperatorType::Source {
                Err(DagError::SourceNotFound)
            } else {
                self.add_operator0(operator, parent_operator_ids, parallelism)
            }
        } else if parent_operator_ids.len() == 1 {
            let p_operator_id = parent_operator_ids[0];
            let (p_node_index, _) = self.operators.get(&p_operator_id).unwrap();
            let p_stream_node = self.dag.index(*p_node_index);

            let p_parallelism = p_stream_node.parallelism;
            let p_operator_type = p_stream_node.operator_type;

            let pipeline =
                self.is_pipeline(operator_type, parallelism, p_operator_type, p_parallelism)?;
            if pipeline {
                // tow types of parallelism inherit
                // 1. Forward:  source->map
                // 2. Backward: window->reduce
                let parallelism = max(parallelism, p_parallelism);
                self.add_operator0(operator, parent_operator_ids, parallelism)
            } else {
                let vir_sink = self.create_virtual_sink(p_parallelism);
                let vir_operator_id =
                    self.add_operator0(vir_sink, vec![p_operator_id], p_parallelism)?;

                let vir_source = self.create_virtual_source(parallelism);
                let vir_operator_id =
                    self.add_operator0(vir_source, vec![vir_operator_id], parallelism)?;

                let vir_operator_id = if self.is_reduce_parent(p_operator_id) {
                    let vir_map = self.create_virtual_flat_map(parallelism);
                    self.add_operator0(vir_map, vec![vir_operator_id], parallelism)?
                } else {
                    vir_operator_id
                };

                self.add_operator0(operator, vec![vir_operator_id], parallelism)
            }
        } else {
            if operator_type != OperatorType::CoProcess {
                return Err(DagError::NotCombineOperator);
            }

            let mut new_p_operator_ids = Vec::new();
            let mut p_reduce_operator_id = None;
            let mut left_parent_parallelism = 0;
            for p_operator_id in parent_operator_ids {
                let (p_node_index, _) = self.operators.get(&p_operator_id).unwrap();
                let p_stream_node = self.dag.index(*p_node_index);

                let parent_is_reduce = p_stream_node.operator_type == OperatorType::Reduce;

                let p_parallelism = p_stream_node.parallelism;
                let vir_sink = self.create_virtual_sink(0);
                let vir_operator_id =
                    self.add_operator0(vir_sink, vec![p_operator_id], p_parallelism)?;

                if parent_is_reduce {
                    p_reduce_operator_id = Some(vir_operator_id)
                } else {
                    new_p_operator_ids.push(vir_operator_id);
                }

                // latest parent is the left parent
                left_parent_parallelism = p_parallelism;
            }

            match p_reduce_operator_id {
                Some(p_reduce_operator_id) => {
                    // left:input_format -> flat_map -> output_format -> input_format -> connect
                    // right:                                                         -> connect

                    let vir_source = self.create_virtual_source(left_parent_parallelism);
                    let vir_operator_id = self.add_operator0(
                        vir_source,
                        vec![p_reduce_operator_id],
                        left_parent_parallelism,
                    )?;

                    let vir_map = self.create_virtual_flat_map(left_parent_parallelism);
                    let vir_operator_id = self.add_operator0(
                        vir_map,
                        vec![vir_operator_id],
                        left_parent_parallelism,
                    )?;

                    let vir_sink = self.create_virtual_sink(left_parent_parallelism);
                    let vir_operator_id = self.add_operator0(
                        vir_sink,
                        vec![vir_operator_id],
                        left_parent_parallelism,
                    )?;

                    // left operator must be the latest index
                    new_p_operator_ids.push(vir_operator_id);

                    let vir_source = self.create_virtual_source(left_parent_parallelism);
                    let vir_operator_id = self.add_operator0(
                        vir_source,
                        new_p_operator_ids,
                        left_parent_parallelism,
                    )?;

                    self.add_operator0(operator, vec![vir_operator_id], left_parent_parallelism)
                }
                None => {
                    let vir_source = self.create_virtual_source(0);
                    let vir_operator_id =
                        self.add_operator0(vir_source, new_p_operator_ids, parallelism)?;

                    self.add_operator0(operator, vec![vir_operator_id], parallelism)
                }
            }
        };
    }

    fn is_pipeline(
        &self,
        operator_type: OperatorType,
        parallelism: u16,
        p_operator_type: OperatorType,
        p_parallelism: u16,
    ) -> Result<bool, DagError> {
        if p_operator_type == OperatorType::WindowAssigner && operator_type == OperatorType::Reduce
        {
            Ok(true)
        } else {
            let pipeline_ip = self.check_operator(operator_type, p_operator_type)?;
            if pipeline_ip && (parallelism == p_parallelism || parallelism == DEFAULT_PARALLELISM) {
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }

    fn check_operator(
        &self,
        operator_type: OperatorType,
        parent_operator_type: OperatorType,
    ) -> Result<bool, DagError> {
        match parent_operator_type {
            OperatorType::Source => match operator_type {
                OperatorType::FlatMap
                | OperatorType::Filter
                | OperatorType::WatermarkAssigner
                | OperatorType::KeyBy
                | OperatorType::Sink => Ok(true),
                OperatorType::Source => Err(DagError::SourceNotAtStarting),
                _ => Ok(false),
            },
            OperatorType::FlatMap | OperatorType::Filter | OperatorType::WatermarkAssigner => {
                match operator_type {
                    OperatorType::FlatMap
                    | OperatorType::Filter
                    | OperatorType::WatermarkAssigner
                    | OperatorType::KeyBy
                    | OperatorType::Sink => Ok(true),
                    OperatorType::Source => Err(DagError::SourceNotAtStarting),
                    _ => Ok(false),
                }
            }
            OperatorType::CoProcess => match operator_type {
                OperatorType::KeyBy => Ok(true),
                OperatorType::Source => Err(DagError::SourceNotAtStarting),
                _ => Ok(false),
            },
            OperatorType::KeyBy => match operator_type {
                OperatorType::Source => Err(DagError::SourceNotAtStarting),
                _ => Ok(false),
            },
            OperatorType::WindowAssigner => match operator_type {
                OperatorType::Reduce => Ok(true),
                OperatorType::Source => Err(DagError::SourceNotAtStarting),
                _ => Ok(false),
            },
            OperatorType::Reduce => match operator_type {
                OperatorType::Source => Err(DagError::SourceNotAtStarting),
                _ => Ok(false),
            },
            OperatorType::Sink => Err(DagError::SinkNotAtEnding),
        }
    }

    fn is_reduce_parent(&self, parent_id: OperatorId) -> bool {
        let (_, operator) = self.operators.get(&parent_id).unwrap();
        OperatorType::from(operator) == OperatorType::Reduce
    }
}
