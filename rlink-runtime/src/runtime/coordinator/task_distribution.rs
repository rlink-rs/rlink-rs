use std::sync::Arc;

use rlink_core::cluster::ManagerStatus;
use rlink_core::context::Args;
use rlink_core::properties::Properties;
use rlink_core::{
    ClusterDescriptor, CoordinatorManagerDescriptor, HeartBeatStatus, TaskDescriptor,
    WorkerManagerDescriptor,
};

use crate::dag::dag_manager::DagManager;

pub(crate) fn build_cluster_descriptor(
    dag_manager: &DagManager,
    application_properties: Arc<Properties>,
    args: Arc<Args>,
) -> ClusterDescriptor {
    let worker_instances = dag_manager
        .physic_graph()
        .alloc_by_instance(args.num_task_managers);

    let mut worker_managers = Vec::new();
    for task_manager_instance in worker_instances {
        let mut task_descriptors = Vec::new();
        for task_instance in &task_manager_instance.task_instances {
            let task_descriptor = TaskDescriptor {
                task_id: task_instance.task_id.clone(),
                terminated: false,
            };
            task_descriptors.push(task_descriptor);
        }

        let task_manager_descriptor = WorkerManagerDescriptor {
            status: ManagerStatus::Pending,
            latest_heart_beat_ts: 0,
            latest_heart_beat_status: HeartBeatStatus::Ok,
            task_manager_id: task_manager_instance.worker_manager_id.clone(),
            // task_manager_address: "".to_string(),
            web_address: "".to_string(),
            task_descriptors,
        };
        worker_managers.push(task_manager_descriptor);
    }

    let coordinator_manager = CoordinatorManagerDescriptor {
        version: crate::utils::VERSION.to_owned(),
        application_id: args.application_id.clone(),
        application_properties: application_properties.as_ref().clone(),
        web_address: "".to_string(),
        status: ManagerStatus::Pending,
        v_cores: args.v_cores,
        memory_mb: args.memory_mb,
        num_task_managers: args.num_task_managers,
        uptime: crate::utils::date_time::current_timestamp_millis(),
        startup_number: 0,
    };

    ClusterDescriptor {
        coordinator_manager,
        worker_managers,
    }
}
