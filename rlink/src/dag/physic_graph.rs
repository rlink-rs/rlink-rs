use crate::dag::execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionNode};
use crate::dag::{TaskId, TaskInstance, WorkerManagerInstance};
use daggy::{EdgeIndex, NodeIndex, Walker};
use std::collections::{HashMap, HashSet};
use std::ops::Index;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct ForwardTaskChain {
    tasks: Vec<ExecutionNode>,
}

impl ForwardTaskChain {
    pub fn new(tasks: Vec<ExecutionNode>) -> Self {
        ForwardTaskChain { tasks }
    }

    pub fn get_task_id(&self) -> TaskId {
        self.tasks
            .iter()
            .min_by_key(|x| x.task_id.job_id)
            .unwrap()
            .task_id
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PhysicGraph {
    /// Map key: first JobId
    task_groups: HashMap<u32, Vec<ForwardTaskChain>>,
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
        let merge_tasks = self.merge_forward_task(execution_graph);

        for (task_id, execution_nodes) in merge_tasks {
            let task_groups = self.task_groups.entry(task_id.job_id).or_insert(vec![]);
            task_groups.push(ForwardTaskChain {
                tasks: execution_nodes,
            });
        }
    }

    pub(crate) fn build0(&mut self, execution_graph: &ExecutionGraph) {
        let chains = self.merge_forward_task0(execution_graph);
        for chain in chains {
            let task_id = chain.get_task_id();
            let task_groups = self.task_groups.entry(task_id.job_id).or_insert(vec![]);
            task_groups.push(chain);
        }
    }

    fn merge_forward_task0(&mut self, execution_graph: &ExecutionGraph) -> Vec<ForwardTaskChain> {
        let execution_dag = &execution_graph.dag;

        let mut all_tasks = HashSet::new();
        let mut forward_tasks = HashSet::new();
        for edge in execution_dag.raw_edges() {
            match edge.weight {
                ExecutionEdge::Memory => {
                    forward_tasks.insert(edge.source());
                    forward_tasks.insert(edge.target());
                }
                ExecutionEdge::Network => {}
            }

            all_tasks.insert(edge.source());
            all_tasks.insert(edge.target());
        }

        let hash_chains: Vec<ForwardTaskChain> = all_tasks
            .into_iter()
            .filter(|node_index| forward_tasks.get(node_index).is_none())
            .map(|node_index| {
                let execution_node = execution_dag.index(node_index).clone();
                ForwardTaskChain {
                    tasks: vec![execution_node],
                }
            })
            .collect();

        let mut forward_tasks: Vec<NodeIndex> = forward_tasks.into_iter().collect();
        let mut forward_chains = Vec::new();
        loop {
            if forward_tasks.len() == 0 {
                break;
            }

            let node_index = forward_tasks.remove(0);
            let parents = self.get_parents(node_index, execution_graph);
            let children = self.get_children(node_index, execution_graph);

            let mut forward_node_indies = Vec::new();
            forward_node_indies.push(node_index);
            forward_node_indies.extend_from_slice(parents.as_slice());
            forward_node_indies.extend_from_slice(children.as_slice());

            let tasks: Vec<ExecutionNode> = forward_node_indies
                .into_iter()
                .map(|node_index| execution_dag.index(node_index).clone())
                .collect();
            forward_chains.push(ForwardTaskChain { tasks });
        }

        forward_chains.extend_from_slice(hash_chains.as_slice());
        forward_chains
    }

    fn get_parents(
        &self,
        node_index: NodeIndex,
        execution_graph: &ExecutionGraph,
    ) -> Vec<NodeIndex> {
        let execution_dag = &execution_graph.dag;
        let parent_node_indies: Vec<NodeIndex> = execution_dag
            .parents(node_index)
            .iter(execution_dag)
            .filter(|(edge, _node)| match execution_dag.index(*edge) {
                ExecutionEdge::Memory => true,
                ExecutionEdge::Network => false,
            })
            .map(|(_edge, node)| node)
            .collect();

        let mut node_indies = Vec::new();
        for node_index in &parent_node_indies {
            let pp_node_indies = self.get_parents(*node_index, execution_graph);
            node_indies.extend_from_slice(pp_node_indies.as_slice());
        }

        node_indies.extend_from_slice(parent_node_indies.as_slice());
        node_indies
    }

    fn get_children(
        &self,
        node_index: NodeIndex,
        execution_graph: &ExecutionGraph,
    ) -> Vec<NodeIndex> {
        let execution_dag = &execution_graph.dag;
        let child_node_indies: Vec<NodeIndex> = execution_dag
            .children(node_index)
            .iter(execution_dag)
            .filter(|(edge, _node)| match execution_dag.index(*edge) {
                ExecutionEdge::Memory => true,
                ExecutionEdge::Network => false,
            })
            .map(|(_edge, node)| node)
            .collect();

        let mut node_indies = Vec::new();
        for node_index in &child_node_indies {
            let cc_node_indies = self.get_children(*node_index, execution_graph);
            node_indies.extend_from_slice(cc_node_indies.as_slice());
        }

        node_indies.extend_from_slice(child_node_indies.as_slice());
        node_indies
    }

    fn merge_forward_task(
        &mut self,
        execution_graph: &ExecutionGraph,
    ) -> HashMap<TaskId, Vec<ExecutionNode>> {
        let execution_dag = &execution_graph.dag;

        let mut jobs = HashMap::new();
        for node in execution_dag.raw_nodes() {
            let execution_node = &node.weight;
            let task_id = execution_node.task_id.clone();
            jobs.insert(task_id, vec![execution_node.clone()]);
        }

        let mut header_nodes = HashSet::new();
        for edge in execution_dag.raw_edges() {
            let mut node_index = edge.source();
            if edge.weight == ExecutionEdge::Memory {
                loop {
                    let parents: Vec<(EdgeIndex, NodeIndex)> = execution_dag
                        .parents(node_index)
                        .iter(execution_dag)
                        .filter(|(edge_index, _n)| {
                            let execution_edge = execution_dag.index(*edge_index);
                            *execution_edge == ExecutionEdge::Memory
                        })
                        .collect();
                    if parents.len() == 0 {
                        break;
                    }
                    if parents.len() > 1 {
                        panic!("too many `Memory` edge");
                    }

                    let parent = parents[0];
                    node_index = parent.1;
                }
                header_nodes.insert(node_index);
            }
        }

        let mut remove_jobs = HashMap::new();
        let mut new_jobs = HashMap::new();

        for header_node_index in header_nodes {
            let mut instances = Vec::new();
            let mut node_index = header_node_index;
            loop {
                let execution_node = execution_dag.index(node_index);
                let task_id = &execution_node.task_id;
                let instance = jobs.get(&task_id).unwrap();
                instances.extend_from_slice(instance.as_slice());
                remove_jobs.insert(*task_id, instance.clone());

                let children: Vec<(EdgeIndex, NodeIndex)> = execution_dag
                    .children(node_index)
                    .iter(execution_dag)
                    .filter(|(edge_index, _n)| {
                        let execution_edge = execution_dag.index(*edge_index);
                        *execution_edge == ExecutionEdge::Memory
                    })
                    .collect();
                if children.len() == 0 {
                    break;
                }
                if children.len() > 1 {
                    panic!("too many `Memory` edge");
                }

                let child = children[0];
                node_index = child.1;
            }

            let header_execution_node = &instances[0];
            let task_id = header_execution_node.task_id.clone();
            new_jobs.insert(task_id, instances);
        }

        remove_jobs.iter().for_each(|(x, _v)| {
            jobs.remove(x);
        });
        new_jobs
            .into_iter()
            .for_each(|(x, v)| match jobs.remove(&x) {
                Some(nodes) => {
                    let mut node_map = HashMap::new();
                    v.into_iter().for_each(|x| {
                        node_map.insert(x.task_id, x);
                    });
                    nodes.into_iter().for_each(|x| {
                        node_map.insert(x.task_id, x);
                    });
                    let nodes: Vec<ExecutionNode> = node_map.into_iter().map(|(_, v)| v).collect();
                    jobs.insert(x, nodes);
                }
                None => {
                    jobs.insert(x, v);
                }
            });

        jobs
    }
}
