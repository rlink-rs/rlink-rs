use crate::api::properties::Properties;
use crate::api::runtime::CheckpointId;
use crate::dag::DagManager;
use crate::runtime::context::Context;
use crate::runtime::HeartBeatStatus;
use crate::runtime::{
    ClusterDescriptor, CoordinatorManagerDescriptor, OperatorDescriptor, TaskDescriptor,
    TaskManagerStatus, WorkerManagerDescriptor,
};

pub(crate) fn build_cluster_descriptor(
    job_name: &str,
    dag_manager: &DagManager,
    application_properties: &Properties,
    context: &Context,
) -> ClusterDescriptor {
    let worker_instances = dag_manager
        .physic_graph()
        .alloc_by_instance(context.num_task_managers);

    let mut worker_managers = Vec::new();
    for task_manager_instance in worker_instances {
        let mut task_descriptors = Vec::new();
        for task_instance in &task_manager_instance.task_instances {
            let operators: Vec<OperatorDescriptor> = task_instance
                .stream_nodes
                .iter()
                .map(|stream_node| OperatorDescriptor {
                    operator_id: stream_node.id,
                    checkpoint_id: CheckpointId::default(),
                    checkpoint_handle: None,
                })
                .collect();

            let task_descriptor = TaskDescriptor {
                task_id: task_instance.task_id.clone(),
                operators,
                input_split: task_instance.input_split.clone(),
                thread_id: "".to_string(),
            };
            task_descriptors.push(task_descriptor);
        }

        let task_manager_descriptor = WorkerManagerDescriptor {
            task_status: TaskManagerStatus::Pending,
            latest_heart_beat_ts: 0,
            latest_heart_beat_status: HeartBeatStatus::Ok,
            task_manager_id: task_manager_instance.worker_manager_id.clone(),
            task_manager_address: "".to_string(),
            metrics_address: "".to_string(),
            web_address: "".to_string(),
            task_descriptors,
        };
        worker_managers.push(task_manager_descriptor);
    }

    let job_manager = CoordinatorManagerDescriptor {
        application_id: context.application_id.clone(),
        application_name: job_name.to_string(),
        application_properties: application_properties.clone(),
        coordinator_address: "".to_string(),
        metrics_address: context.metric_addr.clone(),
        coordinator_status: TaskManagerStatus::Pending,
        v_cores: context.v_cores,
        memory_mb: context.memory_mb,
        num_task_managers: context.num_task_managers,
        uptime: crate::utils::date_time::current_timestamp_millis(),
        startup_number: 0,
    };

    ClusterDescriptor {
        coordinator_manager: job_manager,
        worker_managers,
    }
}
