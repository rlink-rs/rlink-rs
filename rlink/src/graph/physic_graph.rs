use std::collections::HashMap;

use crate::api::operator::StreamOperatorWrap;
use crate::graph::{JobGraph, PhysicGraph, TaskInstance, TaskManagerInstance};

type TaskInstances = Vec<TaskInstance>;
type InSameTaskInstances = Vec<TaskInstances>;
type CrossTaskInstances = Vec<InSameTaskInstances>;

pub fn build_physic_graph(job_graph: &JobGraph, num_task_managers: u32) -> PhysicGraph {
    let chain_tasks = build_chain_task_descriptors(&job_graph);

    let task_managers = build_task_managers(chain_tasks, num_task_managers);

    PhysicGraph {
        task_manager_instances: task_managers,
    }
}

/// Vec<Vec<Vec<TaskDescriptor>>>
///         Task instance descriptor list
///     Chain's Task list which run in the same Task
fn build_chain_task_descriptors(job_graph: &JobGraph) -> CrossTaskInstances {
    let mut cross_task_descriptors = CrossTaskInstances::new();
    for chain_group in &job_graph.chain_groups {
        let mut in_same_task_descriptors = InSameTaskInstances::new();
        for chain in chain_group {
            let first_node = chain.nodes.get(0).unwrap();
            let source_operator = job_graph
                .operators
                .get(first_node.operator_index as usize)
                .expect("no source operator");

            let stream_source = match source_operator {
                StreamOperatorWrap::StreamSource(stream_source) => stream_source,
                _ => panic!("first node must `Source`"),
            };

            let input_splits = stream_source
                .operator_fn
                .create_input_splits(chain.parallelism);
            if input_splits.len() != chain.parallelism as usize {
                panic!("the `InputSplit` size is not equal the `parallelism`")
            }

            let mut tasks = TaskInstances::new();
            let task_len = chain.parallelism as usize;
            for index in 0..task_len {
                let task = TaskInstance {
                    task_id: format!("chain_{}-task_{}", chain.chain_id, index),
                    task_number: index as u16,
                    num_tasks: task_len as u16,
                    chain_id: chain.chain_id.clone(),
                    dependency_chain_id: chain.dependency_chain_id,
                    follower_chain_id: chain.follower_chain_id,
                    dependency_parallelism: chain.dependency_parallelism,
                    follower_parallelism: chain.follower_parallelism,
                    input_split: input_splits[index].clone(),
                };
                tasks.push(task);
            }

            in_same_task_descriptors.push(tasks);
        }

        cross_task_descriptors.push(in_same_task_descriptors);
    }

    cross_task_descriptors
}

fn build_task_managers(
    cross_task_descriptors: CrossTaskInstances,
    num_task_managers: u32,
) -> Vec<TaskManagerInstance> {
    let mut task_managers = Vec::with_capacity(num_task_managers as usize);
    // Init vec
    for index in 0..task_managers.capacity() {
        task_managers.push(TaskManagerInstance {
            task_manager_id: format!("task_manager_{}", index),
            chain_tasks: HashMap::new(),
        })
    }

    let mut task_manager_index = 0;
    for in_same_task_descriptors in &cross_task_descriptors {
        let parallelism = in_same_task_descriptors[0].len();
        for index in 0..parallelism {
            for task_descriptors in in_same_task_descriptors {
                let task = task_descriptors.get(index).unwrap().clone();

                let task_manager_descriptor = task_managers.get_mut(task_manager_index).unwrap();
                let td = task_manager_descriptor.chain_tasks.get_mut(&task.chain_id);
                match td {
                    Some(value) => value.push(task.clone()),
                    None => {
                        task_manager_descriptor
                            .chain_tasks
                            .insert(task.chain_id.clone(), vec![task.clone()]);
                    }
                }
            }

            task_manager_index += 1;
            if task_manager_index == task_managers.len() {
                task_manager_index = 0
            }
        }
    }

    task_managers
}
