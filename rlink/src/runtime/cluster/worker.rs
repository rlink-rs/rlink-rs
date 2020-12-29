use std::time::Duration;

use crate::api::env::{StreamExecutionEnvironment, StreamJob};
use crate::net::worker_service::WorkerServer;
use crate::runtime::context::Context;
use crate::runtime::worker::checkpoint::start_report_checkpoint;
use crate::runtime::worker::heart_beat::{start_heart_beat_timer, status_heartbeat};
use crate::runtime::worker::io::{create_net_channel, get_net_receivers};
use crate::runtime::{worker, JobDescriptor, TaskManagerDescriptor};
use crate::storage::metadata::MetadataLoader;
use crate::utils;
use crate::utils::timer::start_window_timer;

pub(crate) fn run_task<S>(context: Context, stream_env: StreamExecutionEnvironment, stream_job: S)
where
    S: StreamJob + 'static,
{
    let mut metadata_loader = MetadataLoader::new(context.coordinator_address.as_str());

    let job_descriptor = metadata_loader.get_job_descriptor_from_cache();

    let task_manager_id = context.task_manager_id.clone();
    let task_manager_descriptors =
        get_task_manager_descriptor(task_manager_id.as_str(), &job_descriptor).expect(
            format!(
                "TaskManagerDescriptor(task_manager_id={}) is not found",
                task_manager_id.as_str()
            )
            .as_str(),
        );

    // pre-create net channel per chain
    for (chain_id, task_descriptors) in &task_manager_descriptors.chain_tasks {
        create_net_channel(chain_id.clone(), task_descriptors[0].follower_parallelism);
    }

    let mut worker_service = WorkerServer::new(context.bind_ip.to_string());
    worker_service.add_receivers_sync(get_net_receivers());

    let worker_service_clone = worker_service.clone();
    let join_handler = utils::spawn("worker", move || worker_service_clone.serve_sync());
    info!("start serve");

    loop {
        match worker_service.get_bind_addr_sync() {
            Some(addr) => {
                status_heartbeat(
                    job_descriptor.job_manager.coordinator_address.as_str(),
                    task_manager_id.as_str(),
                    addr.to_string().as_str(),
                    context.metric_addr.as_str(),
                );

                // heat beat timer
                start_heart_beat_timer(
                    job_descriptor.job_manager.coordinator_address.as_str(),
                    task_manager_id.as_str(),
                    addr.to_string().as_str(),
                    context.metric_addr.as_str(),
                );

                // report checkpoint timer
                start_report_checkpoint(job_descriptor.job_manager.coordinator_address.as_str());

                break;
            }
            None => {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }

    let window_timer = start_window_timer();

    for (chain_id, task_descriptors) in &task_manager_descriptors.chain_tasks {
        info!("submit task chain_id={}", chain_id);
        for task_descriptor in task_descriptors {
            let window_timer_clone = window_timer.clone();
            worker::run(
                context.clone(),
                metadata_loader.clone(),
                task_descriptor.clone(),
                stream_job.clone(),
                stream_env.clone(),
                window_timer_clone,
            );
        }
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
    job_descriptor: &JobDescriptor,
) -> Option<TaskManagerDescriptor> {
    for task_manager_descriptors in &job_descriptor.task_managers {
        if task_manager_descriptors.task_manager_id.eq(task_manager_id) {
            return Some(task_manager_descriptors.clone());
        }
    }

    None
}

// fn _net_bootstrap(
//     task_manager_id: String,
//     metadata_storage_mode: MetadataStorageMode,
//     bind_ip: String,
// ) -> JoinHandle<std::io::Result<()>> {
//     let mut worker_service = WorkerServer::new(bind_ip);
//     worker_service.add_receivers_sync(get_net_receivers());
//
//     let worker_service_clone = worker_service.clone();
//     let join_handler = utils::spawn("worker", move || worker_service_clone.serve_sync());
//     info!("start serve");
//
//     loop {
//         match worker_service.get_bind_addr_sync() {
//             Some(addr) => {
//                 let metadata_storage = MetadataStorageWrap::new(&metadata_storage_mode);
//                 metadata_storage.update_task_status(
//                     task_manager_id,
//                     addr.to_string(),
//                     TaskManagerStatus::Registered,
//                 );
//                 break;
//             }
//             None => {
//                 std::thread::sleep(Duration::from_secs(1));
//             }
//         }
//     }
//
//     join_handler
// }
