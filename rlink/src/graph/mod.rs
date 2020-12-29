use std::collections::HashMap;

use crate::api::data_stream::StreamGraph;
use crate::api::function::InputSplit;
use crate::api::operator::{StreamOperatorWrap, TStreamOperator};
use crate::graph::execution_graph::build_logic_plan_group;
use crate::graph::job_graph::build_job_graph;
use crate::graph::job_graph_revise::revise_logic_graph;
use crate::storage::metadata::MetadataLoader;

pub mod execution_graph;
pub mod job_graph;
pub mod job_graph_revise;
pub mod physic_graph;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct GraphNode {
    pub(crate) name: String,
    pub(crate) node_id: u32,
    pub(crate) parent_node_id: u32,
    pub(crate) parallelism: u32,
    // pub(crate) vertex: bool,
    pub(crate) operator_index: u32,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ChainEdge {
    InSameTask = 1,
    CrossTask = 2,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct OperatorChain {
    pub(crate) chain_id: u32,
    pub(crate) dependency_chain_id: u32,
    pub(crate) follower_chain_id: u32,
    pub(crate) dependency_edge: ChainEdge,
    pub(crate) follower_edge: ChainEdge,
    pub(crate) parallelism: u32,
    pub(crate) dependency_parallelism: u32,
    pub(crate) follower_parallelism: u32,
    pub(crate) nodes: Vec<GraphNode>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobGraph {
    /// Map(chain_id, Group(OperatorChain))
    pub(crate) chain_map: HashMap<u32, OperatorChain>,
    pub(crate) chain_groups: Vec<Vec<OperatorChain>>,
    #[serde(skip_serializing, skip_deserializing)]
    pub(crate) operators: Vec<StreamOperatorWrap>,
}

impl JobGraph {
    pub fn pop_stream_operator(&mut self, operator_id: u32) -> StreamOperatorWrap {
        for index in 0..self.operators.len() {
            match self.operators.get(index) {
                Some(stream_operator) => {
                    if stream_operator.get_operator_id() == operator_id {
                        return self.operators.remove(index);
                    }
                }
                None => {}
            }
        }
        panic!(format!("can not find operator. id={}", operator_id));
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskInstance {
    pub task_id: String,
    /// task sequence number. each chain，task sequence number start at 0.
    pub task_number: u16,
    /// total number tasks in the chain.
    pub num_tasks: u16,
    pub chain_id: u32,
    pub dependency_chain_id: u32,
    pub follower_chain_id: u32,
    pub dependency_parallelism: u32,
    pub follower_parallelism: u32,
    pub input_split: InputSplit,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskManagerInstance {
    /// build by self, format `format!("task_manager_{}", index)`
    pub task_manager_id: String,
    /// chain tasks map: <chain_id, Vec<TaskInstance>>
    pub chain_tasks: HashMap<u32, Vec<TaskInstance>>,
}

#[derive(Debug)]
pub struct PhysicGraph {
    pub task_manager_instances: Vec<TaskManagerInstance>,
}

pub(crate) fn build_logic_plan<T: StreamGraph>(
    data_stream: T,
    metadata_loader: MetadataLoader,
) -> JobGraph {
    let operators = data_stream.into_operators();

    let logic_plan = build_job_graph(operators);
    let revise_plan = revise_logic_graph(logic_plan, metadata_loader);
    build_logic_plan_group(revise_plan)
}

pub use physic_graph::build_physic_graph;
