use std::borrow::{Borrow, BorrowMut};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::core::env::StreamApp;
use crate::core::runtime::{ClusterDescriptor, ManagerStatus, WorkerManagerDescriptor};
use crate::dag::metadata::DagMetadata;
use crate::pub_sub::network;
use crate::runtime::context::Context;
use crate::runtime::timer::{start_window_timer, WindowTimer};
use crate::runtime::worker::checkpoint::CheckpointPublish;
use crate::runtime::worker::heart_beat::HeartbeatPublish;
use crate::runtime::worker::web_server::web_launch;
use crate::runtime::worker::WorkerTaskContext;
use crate::runtime::{worker, HeartBeatStatus, HeartbeatItem};
use crate::storage::metadata::MetadataLoader;

pub(crate) async fn run<S>(context: Arc<Context>, stream_app: S) -> anyhow::Result<()>
where
    S: StreamApp + 'static,
{
    let mut metadata_loader = MetadataLoader::new(context.coordinator_address.as_str());

    let cluster_descriptor = metadata_loader.get_cluster_descriptor().await;
    info!("preload `ClusterDescriptor`");

    let server_addr = bootstrap_publish_serve(context.bind_ip.to_string()).await;
    info!("bootstrap publish server, listen: {}", server_addr);

    let web_address = web_serve(context.clone()).await;
    info!("serve worker web ui {}", web_address);

    let checkpoint_publish = start_checkpoint_task(&cluster_descriptor).await;
    info!("start checkpoint timer");

    let window_timer = start_window_timer().await;
    info!("bootstrap window timer");

    let heartbeat_publish = start_heartbeat_task(
        &cluster_descriptor,
        context.deref(),
        server_addr,
        web_address.as_str(),
    )
    .await;
    info!("start heartbeat timer and register worker to coordinator");

    let cluster_descriptor = waiting_all_task_manager_fine(metadata_loader.borrow_mut()).await;
    info!("all task manager is fine");

    let dag_metadata = load_dag_metadata(metadata_loader.borrow_mut()).await;
    info!("load dag metadata success");

    let join_handles = run_tasks(
        cluster_descriptor.clone(),
        dag_metadata,
        context.clone(),
        window_timer,
        checkpoint_publish,
        heartbeat_publish.clone(),
        stream_app,
    )
    .await;
    info!("all task has bootstrap");

    for join_handle in join_handles {
        join_handle.await.unwrap();
    }

    stop_heartbeat_timer(heartbeat_publish.deref()).await;
    info!("work end");

    Ok(())
}

fn get_worker_manager_descriptor(
    task_manager_id: &str,
    cluster_descriptor: &ClusterDescriptor,
) -> Option<WorkerManagerDescriptor> {
    for task_manager_descriptors in &cluster_descriptor.worker_managers {
        if task_manager_descriptors.task_manager_id.eq(task_manager_id) {
            return Some(task_manager_descriptors.clone());
        }
    }

    None
}

async fn bootstrap_publish_serve(bind_ip: String) -> SocketAddr {
    let worker_service = network::Server::new(bind_ip);
    let worker_service_clone = worker_service.clone();
    tokio::spawn(async move {
        // TODO error handle
        worker_service_clone.serve().await.unwrap();
    });
    loop {
        match worker_service.bind_addr().await {
            Some(addr) => {
                return addr;
            }
            None => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
}

async fn web_serve(context: Arc<Context>) -> String {
    let address = web_launch(context).await;
    address
}

async fn start_heartbeat_task(
    cluster_descriptor: &ClusterDescriptor,
    context: &Context,
    bind_addr: SocketAddr,
    web_addr: &str,
) -> Arc<HeartbeatPublish> {
    let coordinator_address = cluster_descriptor.coordinator_manager.web_address.clone();
    let task_manager_id = context.task_manager_id.clone();

    let heartbeat_publish =
        HeartbeatPublish::new(coordinator_address.clone(), task_manager_id.clone()).await;

    let status = HeartbeatItem::WorkerAddrs {
        address: bind_addr.to_string(),
        web_address: web_addr.to_string(),
        metrics_address: context.metric_addr.clone(),
    };
    heartbeat_publish.report(status).await;

    heartbeat_publish.start_heartbeat_timer().await;

    Arc::new(heartbeat_publish)
}

async fn start_checkpoint_task(cluster_descriptor: &ClusterDescriptor) -> Arc<CheckpointPublish> {
    let coordinator_address = cluster_descriptor.coordinator_manager.web_address.clone();
    let checkpoint_publish = CheckpointPublish::new(coordinator_address).await;
    Arc::new(checkpoint_publish)
}

async fn stop_heartbeat_timer(heartbeat_publish: &HeartbeatPublish) {
    let status = HeartbeatItem::HeartBeatStatus(HeartBeatStatus::End);
    heartbeat_publish.report(status).await;
}

async fn waiting_all_task_manager_fine(
    metadata_loader: &mut MetadataLoader,
) -> Arc<ClusterDescriptor> {
    Arc::new(waiting_all_task_manager_fine0(metadata_loader).await)
}

async fn waiting_all_task_manager_fine0(metadata_loader: &mut MetadataLoader) -> ClusterDescriptor {
    loop {
        let cluster_descriptor = metadata_loader.get_cluster_descriptor().await;
        match cluster_descriptor.coordinator_manager.status {
            ManagerStatus::Registered => {
                return cluster_descriptor;
            }
            _ => tokio::time::sleep(Duration::from_secs(2)).await,
        }
    }
}

async fn load_dag_metadata(metadata_loader: &mut MetadataLoader) -> Arc<DagMetadata> {
    Arc::new(metadata_loader.get_dag_metadata().await)
}

async fn run_tasks<S>(
    cluster_descriptor: Arc<ClusterDescriptor>,
    dag_metadata: Arc<DagMetadata>,
    context: Arc<Context>,
    window_timer: WindowTimer,
    checkpoint_publish: Arc<CheckpointPublish>,
    heartbeat_publish: Arc<HeartbeatPublish>,
    stream_app: S,
) -> Vec<JoinHandle<()>>
where
    S: StreamApp + 'static,
{
    let task_manager_id = context.task_manager_id.as_str();
    // todo error check
    let task_manager_descriptors =
        get_worker_manager_descriptor(task_manager_id, cluster_descriptor.borrow()).unwrap();

    let mut join_handles = Vec::with_capacity(task_manager_descriptors.task_descriptors.len());
    for task_descriptor in &task_manager_descriptors.task_descriptors {
        let task_context = WorkerTaskContext::new(
            context.clone(),
            dag_metadata.clone(),
            cluster_descriptor.clone(),
            task_descriptor.clone(),
            window_timer.clone(),
            checkpoint_publish.clone(),
            heartbeat_publish.clone(),
        );

        let join_handle = worker::run(Arc::new(task_context), stream_app.clone()).await;
        join_handles.push(join_handle);
    }

    join_handles
}
