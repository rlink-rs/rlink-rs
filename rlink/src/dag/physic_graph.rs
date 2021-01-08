use crate::dag::execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionNode};
use crate::dag::{TaskId, TaskInstance, WorkerManagerInstance};
use daggy::{EdgeIndex, NodeIndex, Walker};
use std::collections::{HashMap, HashSet};
use std::ops::Index;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct ForwardTaskChain {
    tasks: Vec<ExecutionNode>,
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

        for header_node_index in header_nodes {
            let mut instances = Vec::new();
            let mut node_index = header_node_index;
            loop {
                let execution_node = execution_dag.index(node_index);
                let task_id = &execution_node.task_id;
                let instance = jobs.remove(&task_id).unwrap();
                instances.extend_from_slice(instance.as_slice());

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
            jobs.insert(task_id, instances);
        }

        jobs
    }
}
