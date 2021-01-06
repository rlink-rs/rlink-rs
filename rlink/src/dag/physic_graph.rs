use crate::dag::execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionNode};
use crate::dag::TaskId;
use daggy::{EdgeIndex, NodeIndex, Walker};
use std::collections::{HashMap, HashSet};
use std::ops::Index;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct TaskGroup {
    tasks: Vec<ExecutionNode>,
}

pub(crate) struct PhysicGraph {}

impl PhysicGraph {
    pub fn new() -> Self {
        PhysicGraph {}
    }

    pub(crate) fn build(
        &mut self,
        execution_graph: &ExecutionGraph,
    ) -> HashMap<u32, Vec<TaskGroup>> {
        let merge_tasks = self.merge_forward_task(execution_graph);

        let mut task_groups = HashMap::new();
        for (task_id, execution_nodes) in merge_tasks {
            let task_groups = task_groups.entry(task_id.job_id).or_insert(vec![]);
            task_groups.push(TaskGroup {
                tasks: execution_nodes,
            });
        }

        task_groups
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
