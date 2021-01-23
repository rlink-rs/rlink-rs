use std::borrow::BorrowMut;
use std::net::SocketAddr;
use std::thread::JoinHandle;
use std::time::Duration;

use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::pub_sub::network;
use crate::runtime::context::Context;
use crate::runtime::timer::{start_window_timer, WindowTimer};
use crate::runtime::worker::checkpoint::start_report_checkpoint;
use crate::runtime::worker::heart_beat::{start_heart_beat_timer, status_heartbeat};
use crate::runtime::{worker, ApplicationDescriptor, TaskManagerStatus, WorkerManagerDescriptor};
use crate::storage::metadata::MetadataLoader;
use crate::utils;

pub(crate) fn run<S>(context: Context, stream_env: StreamExecutionEnvironment, stream_app: S)
where
    S: StreamApp + 'static,
{
    let mut metadata_loader = MetadataLoader::new(context.coordinator_address.as_str());

    let application_descriptor = metadata_loader.get_application_descriptor();

    let server_addr = bootstrap_publish_serve(context.bind_ip.to_string());
    info!("bootstrap publish server, listen: {}", server_addr);

    bootstrap_timer_task(&application_descriptor, &context, server_addr);
    info!("bootstrap timer task");

    let window_timer = start_window_timer();
    info!("bootstrap window timer");

    waiting_all_task_manager_fine(metadata_loader.borrow_mut());
    info!("all task manager is fine");

    // after all task fine. and reload `ApplicationDescriptor` from metadata
    let application_descriptor = metadata_loader.get_application_descriptor();

    bootstrap_subscribe_client(&application_descriptor);
    info!("bootstrap subscribe client");

    let join_handles = run_tasks(
        &application_descriptor,
        &context,
        metadata_loader,
        window_timer,
        stream_env,
        stream_app,
    );
    info!("all task has bootstrap");

    join_handles.into_iter().for_each(|join_handle| {
        join_handle.join().unwrap();
    });
    info!("work end");
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

fn bootstrap_publish_serve(bind_ip: String) -> SocketAddr {
    let worker_service = network::Server::new(bind_ip);
    let worker_service_clone = worker_service.clone();
    utils::thread::spawn("publish_serve", move || worker_service_clone.serve_sync());
    loop {
        match worker_service.get_bind_addr_sync() {
            Some(addr) => {
                return addr;
            }
            None => std::thread::sleep(Duration::from_secs(1)),
        }
    }
}

fn bootstrap_subscribe_client(application_descriptor: &ApplicationDescriptor) {
    let application_descriptor = application_descriptor.clone();
    utils::thread::spawn("subscribe_client", move || {
        network::run_subscribe(application_descriptor)
    });
}

fn bootstrap_timer_task(
    application_descriptor: &ApplicationDescriptor,
    context: &Context,
    bind_addr: SocketAddr,
) {
    let coordinator_address = application_descriptor
        .coordinator_manager
        .coordinator_address
        .as_str();

    status_heartbeat(
        coordinator_address,
        context.task_manager_id.as_str(),
        bind_addr.to_string().as_str(),
        context.metric_addr.as_str(),
    );

    // heat beat timer
    start_heart_beat_timer(
        coordinator_address,
        context.task_manager_id.as_str(),
        bind_addr.to_string().as_str(),
        context.metric_addr.as_str(),
    );

    // report checkpoint timer
    start_report_checkpoint(coordinator_address);
}

fn waiting_all_task_manager_fine(metadata_loader: &mut MetadataLoader) {
    loop {
        let job_descriptor = metadata_loader.get_application_descriptor();
        match job_descriptor.coordinator_manager.coordinator_status {
            TaskManagerStatus::Registered => {
                break;
            }
            _ => std::thread::sleep(Duration::from_secs(2)),
        }
    }
}

fn run_tasks<S>(
    application_descriptor: &ApplicationDescriptor,
    context: &Context,
    metadata_loader: MetadataLoader,
    window_timer: WindowTimer,
    stream_env: StreamExecutionEnvironment,
    stream_app: S,
) -> Vec<JoinHandle<()>>
where
    S: StreamApp + 'static,
{
    let task_manager_id = context.task_manager_id.as_str();
    // todo error check
    let task_manager_descriptors =
        get_task_manager_descriptor(task_manager_id, &application_descriptor).unwrap();

    task_manager_descriptors
        .task_descriptors
        .iter()
        .map(|task_descriptor| {
            worker::run(
                context.clone(),
                metadata_loader.clone(),
                task_descriptor.clone(),
                stream_app.clone(),
                &stream_env,
                window_timer.clone(),
            )
        })
        .collect()
}
