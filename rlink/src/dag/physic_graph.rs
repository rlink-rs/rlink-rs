use std::collections::{HashMap, HashSet};
use std::ops::Index;

use daggy::{Dag, EdgeIndex, NodeIndex, Walker};

use crate::api::runtime::JobId;
use crate::dag::execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionNode};
use crate::dag::{TaskId, TaskInstance, WorkerManagerInstance};
use std::borrow::BorrowMut;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct ForwardTaskChain {
    tasks: Vec<ExecutionNode>,
}

impl ForwardTaskChain {
    pub fn get_task_id(&self) -> TaskId {
        self.tasks
            .iter()
            .min_by_key(|x| x.task_id.job_id.0)
            .unwrap()
            .task_id
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PhysicGraph {
    /// Map key: first JobId
    task_groups: HashMap<JobId, Vec<ForwardTaskChain>>,
}

impl PhysicGraph {
    pub fn new() -> Self {
        PhysicGraph {
            task_groups: HashMap::new(),
        }
    }

    pub(crate) fn alloc_by_instance(&self, num_task_managers: u32) -> Vec<WorkerManagerInstance> {
        let mut task_managers = Vec::with_capacity(num_task_managers as usize);
        for index in 0..task_managers.capacity() {
            task_managers.push(WorkerManagerInstance {
                worker_manager_id: format!("worker_manager_{}", index),
                task_instances: Vec::new(),
            })
        }

        let mut i = 0;
        for (_first_job_id, forward_task_chain) in &self.task_groups {
            for chain in forward_task_chain {
                let index = i % task_managers.len();
                i += 1;
                let task_manager = &mut task_managers[index];

                for execution_node in &chain.tasks {
                    task_manager.task_instances.push(TaskInstance {
                        task_id: execution_node.task_id.clone(),
                        input_split: execution_node.input_split.clone(),
                    });
                }
            }
        }

        task_managers
    }

    pub(crate) fn build(&mut self, execution_graph: &ExecutionGraph) {
        let chains = self.merge_forward_task(execution_graph);
        for chain in chains {
            let task_id = chain.get_task_id();
            let task_groups = self.task_groups.entry(task_id.job_id).or_insert(vec![]);
            task_groups.push(chain);
        }
    }

    fn merge_forward_task(&mut self, execution_graph: &ExecutionGraph) -> Vec<ForwardTaskChain> {
        let execution_dag = &execution_graph.dag;

        let mut all_task_set = HashSet::new();
        let mut forward_task_set = HashSet::new();
        for edge in execution_dag.raw_edges() {
            match edge.weight {
                ExecutionEdge::Memory => {
                    forward_task_set.insert(edge.source());
                    forward_task_set.insert(edge.target());
                }
                ExecutionEdge::Network => {}
            }

            all_task_set.insert(edge.source());
            all_task_set.insert(edge.target());
        }

        let hash_chains: Vec<ForwardTaskChain> = all_task_set
            .into_iter()
            .filter(|node_index| forward_task_set.get(node_index).is_none())
            .map(|node_index| {
                let execution_node = execution_dag.index(node_index).clone();
                ForwardTaskChain {
                    tasks: vec![execution_node],
                }
            })
            .collect();

        let mut forward_chains = Vec::new();
        loop {
            let first_node_index = forward_task_set.iter().next().map(|x| *x);
            match first_node_index {
                Some(node_index) => {
                    let mut dag = execution_dag.clone();
                    let forward_node_indies = self.search(node_index, dag.borrow_mut());

                    forward_node_indies.iter().for_each(|x| {
                        forward_task_set.remove(x);
                    });

                    let tasks: Vec<ExecutionNode> = forward_node_indies
                        .into_iter()
                        .map(|node_index| execution_dag.index(node_index).clone())
                        .collect();
                    forward_chains.push(ForwardTaskChain { tasks });
                }
                None => break,
            }
        }

        forward_chains.extend_from_slice(hash_chains.as_slice());
        forward_chains
    }

    fn search(
        &self,
        node_index: NodeIndex,
        execution_dag: &mut Dag<ExecutionNode, ExecutionEdge>,
    ) -> Vec<NodeIndex> {
        let mut node_indies = Vec::new();
        node_indies.push(node_index);

        let parents = self.get_parents(node_index, execution_dag);
        for p in parents {
            let p_indies = self.search(p, execution_dag);
            node_indies.extend_from_slice(p_indies.as_slice());
        }

        let children = self.get_children(node_index, execution_dag);
        for p in children {
            let p_indies = self.search(p, execution_dag);
            node_indies.extend_from_slice(p_indies.as_slice());
        }

        node_indies
    }

    fn get_parents(
        &self,
        node_index: NodeIndex,
        execution_dag: &mut Dag<ExecutionNode, ExecutionEdge>,
    ) -> Vec<NodeIndex> {
        let parent_node_indies: Vec<(EdgeIndex, NodeIndex)> = execution_dag
            .parents(node_index)
            .iter(execution_dag)
            .filter(|(edge, _node)| match execution_dag.index(*edge) {
                ExecutionEdge::Memory => true,
                ExecutionEdge::Network => false,
            })
            .collect();

        let parent_node_indies: Vec<NodeIndex> = parent_node_indies
            .into_iter()
            .map(|(edge, node)| {
                execution_dag.remove_edge(edge);
                node
            })
            .collect();

        parent_node_indies
    }

    fn get_children(
        &self,
        node_index: NodeIndex,
        execution_dag: &mut Dag<ExecutionNode, ExecutionEdge>,
    ) -> Vec<NodeIndex> {
        let child_node_indies: Vec<(EdgeIndex, NodeIndex)> = execution_dag
            .children(node_index)
            .iter(execution_dag)
            .filter(|(edge, _node)| match execution_dag.index(*edge) {
                ExecutionEdge::Memory => true,
                ExecutionEdge::Network => false,
            })
            .collect();

        let child_node_indies: Vec<NodeIndex> = child_node_indies
            .into_iter()
            .map(|(edge, node)| {
                execution_dag.remove_edge(edge);
                node
            })
            .collect();

        child_node_indies
    }
}
