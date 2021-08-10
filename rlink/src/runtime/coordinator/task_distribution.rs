use crate::core::properties::Properties;
use crate::core::runtime::{
    CheckpointId, ClusterDescriptor, CoordinatorManagerDescriptor, ManagerStatus,
    OperatorDescriptor, TaskDescriptor, WorkerManagerDescriptor,
};
use crate::dag::DagManager;
use crate::runtime::context::Context;
use crate::runtime::HeartBeatStatus;

pub(crate) fn build_cluster_descriptor(
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
                    input_schema: stream_node.input_schema.clone(),
                    output_schema: stream_node.output_schema.clone(),
                    checkpoint_id: CheckpointId::default(),
                    completed_checkpoint_id: None,
                    checkpoint_handle: None,
                })
                .collect();

            let task_descriptor = TaskDescriptor {
                task_id: task_instance.task_id.clone(),
                operators,
                input_split: task_instance.input_split.clone(),
                daemon: task_instance.daemon,
                thread_id: "".to_string(),
                terminated: false,
            };
            task_descriptors.push(task_descriptor);
        }

        let task_manager_descriptor = WorkerManagerDescriptor {
            task_status: ManagerStatus::Pending,
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

    let coordinator_manager = CoordinatorManagerDescriptor {
        version: crate::utils::VERSION.to_owned(),
        application_id: context.application_id.clone(),
        application_properties: application_properties.clone(),
        web_address: "".to_string(),
        metrics_address: context.metric_addr.clone(),
        coordinator_status: ManagerStatus::Pending,
        v_cores: context.v_cores,
        memory_mb: context.memory_mb,
        num_task_managers: context.num_task_managers,
        uptime: crate::utils::date_time::current_timestamp_millis(),
        startup_number: 0,
    };

    ClusterDescriptor {
        coordinator_manager,
        worker_managers,
    }
}
