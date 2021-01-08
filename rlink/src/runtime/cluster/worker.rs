use std::time::Duration;

use crate::api::env::{StreamExecutionEnvironment, StreamJob};
use crate::io::network;
use crate::runtime::context::Context;
use crate::runtime::worker::checkpoint::start_report_checkpoint;
use crate::runtime::worker::heart_beat::{start_heart_beat_timer, status_heartbeat};
use crate::runtime::{worker, ApplicationDescriptor, WorkerManagerDescriptor};
use crate::storage::metadata::MetadataLoader;
use crate::utils;
use crate::utils::timer::start_window_timer;

pub(crate) fn run_task<S>(context: Context, stream_env: StreamExecutionEnvironment, stream_job: S)
where
    S: StreamJob + 'static,
{
    let mut metadata_loader = MetadataLoader::new(context.coordinator_address.as_str());

    let application_descriptor = metadata_loader.get_job_descriptor_from_cache();

    // stream_job.build_stream(&application_properties, stream_env.borrow_mut());
    //
    // let mut raw_stream_graph = stream_env.stream_manager.stream_graph.borrow_mut();
    // let dag_manager = DagManager::new(raw_stream_graph.deref());
    // info!("StreamGraph: {:?}", dag_manager.stream_graph().dag);
    // info!("JobGraph: {:?}", dag_manager.job_graph().dag);
    // info!("ExecutionGraph: {:?}", dag_manager.execution_graph().dag);

    let task_manager_id = context.task_manager_id.as_str();
    let task_manager_descriptors =
        get_task_manager_descriptor(task_manager_id, &application_descriptor).expect(
            format!(
                "TaskManagerDescriptor(task_manager_id={}) is not found",
                task_manager_id
            )
            .as_str(),
        );

    // todo bootstrap server
    let worker_service = network::Server::new(context.bind_ip.to_string());

    let worker_service_clone = worker_service.clone();
    let join_handler = utils::spawn("worker", move || worker_service_clone.serve_sync());
    info!("start serve");

    loop {
        match worker_service.get_bind_addr_sync() {
            Some(addr) => {
                status_heartbeat(
                    application_descriptor
                        .coordinator_manager
                        .coordinator_address
                        .as_str(),
                    context.task_manager_id.as_str(),
                    addr.to_string().as_str(),
                    context.metric_addr.as_str(),
                );

                // heat beat timer
                start_heart_beat_timer(
                    application_descriptor
                        .coordinator_manager
                        .coordinator_address
                        .as_str(),
                    context.task_manager_id.as_str(),
                    addr.to_string().as_str(),
                    context.metric_addr.as_str(),
                );

                // report checkpoint timer
                start_report_checkpoint(
                    application_descriptor
                        .coordinator_manager
                        .coordinator_address
                        .as_str(),
                );

                break;
            }
            None => {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }

    let window_timer = start_window_timer();

    for task_descriptor in &task_manager_descriptors.task_descriptors {
        info!("submit task {:?}", task_descriptor);

        let window_timer_clone = window_timer.clone();
        worker::run(
            context.clone(),
            metadata_loader.clone(),
            task_descriptor.clone(),
            stream_job.clone(),
            &stream_env,
            window_timer_clone,
        );
    }

    let result = join_handler
        .join()
        .expect("Couldn't join on the associated thread");
    match result {
        Ok(_) => {
            info!("work end");
        }
        Err(e) => {
            info!("work end with error: {}", e);
        }
    }
}

fn get_task_manager_descriptor(
    task_manager_id: &str,
    application_descriptor: &ApplicationDescriptor,
) -> Option<WorkerManagerDescriptor> {
    for task_manager_descriptors in &application_descriptor.worker_managers {
        if task_manager_descriptors.task_manager_id.eq(task_manager_id) {
            return Some(task_manager_descriptors.clone());
        }
    }

    None
}
