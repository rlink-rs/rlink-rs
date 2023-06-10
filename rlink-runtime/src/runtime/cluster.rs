use std::sync::Arc;

use rlink_core::cluster::ManagerType;
use rlink_core::context::Args;

use crate::deployment::ResourceManager;
use crate::env::ApplicationEnv;
use crate::runtime::coordinator::CoordinatorTask;
use crate::runtime::worker::WorkerTask;

pub async fn run_task(args: Arc<Args>, env: Arc<ApplicationEnv>) -> anyhow::Result<()> {
    info!("num of cpu cores: {}", num_cpus::get());
    match args.manager_type {
        ManagerType::Coordinator => run_coordinator(args, env).await,
        ManagerType::Worker => run_worker(args, env).await,
    }
}

async fn run_coordinator(args: Arc<Args>, env: Arc<ApplicationEnv>) -> anyhow::Result<()> {
    let resource_manager = ResourceManager::new(args.clone());
    let mut coordinator_task = CoordinatorTask::new(args, env, resource_manager);
    coordinator_task.run().await
}

async fn run_worker(args: Arc<Args>, env: Arc<ApplicationEnv>) -> anyhow::Result<()> {
    let worker_task = WorkerTask::new(args, env);
    worker_task.run().await
}
