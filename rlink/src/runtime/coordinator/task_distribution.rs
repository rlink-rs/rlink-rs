use crate::api::properties::Properties;
use crate::graph::{build_physic_graph, JobGraph};
use crate::runtime::context::Context;
use crate::runtime::{
    JobDescriptor, JobManagerDescriptor, TaskDescriptor, TaskManagerDescriptor, TaskManagerStatus,
};
use std::collections::HashMap;

pub(crate) fn build_job_descriptor(
    job_name: &str,
    logic_plan: &JobGraph,
    job_properties: &Properties,
    context: &Context,
) -> JobDescriptor {
    let physic_plan = build_physic_graph(logic_plan, context.num_task_managers);

    let mut task_managers = Vec::new();
    for task_manager_instance in &physic_plan.task_manager_instances {
        let mut chain_tasks = HashMap::new();
        for (chain_id, task_instances) in &task_manager_instance.chain_tasks {
            let task_descriptors: Vec<TaskDescriptor> = task_instances
                .iter()
                .map(|task_instance| TaskDescriptor {
                    task_id: task_instance.task_id.clone(),
                    task_number: task_instance.task_number,
                    num_tasks: task_instance.num_tasks,
                    chain_id: task_instance.chain_id,
                    dependency_chain_id: task_instance.dependency_chain_id,
                    follower_chain_id: task_instance.follower_chain_id,
                    dependency_parallelism: task_instance.dependency_parallelism,
                    follower_parallelism: task_instance.follower_parallelism,
                    input_split: task_instance.input_split.clone(),
                    checkpoint_id: 0,
                    checkpoint_handle: None,
                })
                .collect();
            chain_tasks.insert(chain_id.clone(), task_descriptors);
        }

        let task_manager_descriptor = TaskManagerDescriptor {
            task_status: TaskManagerStatus::Pending,
            latest_heart_beat_ts: 0,
            task_manager_id: task_manager_instance.task_manager_id.clone(),
            task_manager_address: "".to_string(),
            metrics_address: "".to_string(),
            cpu_cores: 0,
            physical_memory: 0,
            chain_tasks,
        };
        task_managers.push(task_manager_descriptor);
    }

    let job_manager = JobManagerDescriptor {
        job_id: context.job_id.clone(),
        job_name: job_name.to_string(),
        job_properties: job_properties.clone(),
        coordinator_address: "".to_string(),
        job_status: TaskManagerStatus::Pending,
    };

    JobDescriptor {
        job_manager,
        task_managers,
    }
}
