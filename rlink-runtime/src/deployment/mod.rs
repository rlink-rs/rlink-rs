use std::sync::Arc;

use rlink_core::cluster::{ClusterMode, TaskResourceInfo};
use rlink_core::context::Args;
use rlink_core::ClusterDescriptor;

use crate::deployment::kubernetes::KubernetesResourceManager;
use crate::deployment::local::LocalResourceManager;
use crate::deployment::standalone::StandaloneResourceManager;
use crate::deployment::yarn::YarnResourceManager;
use crate::env::ApplicationEnv;

pub mod kubernetes;
pub mod local;
pub mod standalone;
pub mod yarn;

pub struct Resource {
    memory: u32,
    cpu_cores: u32,
}

impl Resource {
    pub fn new(memory: u32, cpu_cores: u32) -> Self {
        Resource { memory, cpu_cores }
    }
}

#[async_trait]
pub(crate) trait TResourceManager {
    fn prepare(&mut self, context: &Args, job_descriptor: &ClusterDescriptor);

    /// worker resource allocate
    /// Return a resource location.
    async fn worker_allocate(
        &self,
        env: Arc<ApplicationEnv>,
    ) -> anyhow::Result<Vec<TaskResourceInfo>>;

    async fn stop_workers(&self, task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()>;
}

pub(crate) enum ResourceManager {
    LocalResourceManager(LocalResourceManager),
    StandaloneResourceManager(StandaloneResourceManager),
    YarnResourceManager(YarnResourceManager),
    KubernetesResourceManager(KubernetesResourceManager),
}

impl ResourceManager {
    pub fn new(args: Arc<Args>) -> Self {
        match args.cluster_mode {
            ClusterMode::Local => {
                ResourceManager::LocalResourceManager(LocalResourceManager::new(args.clone()))
            }
            ClusterMode::Standalone => ResourceManager::StandaloneResourceManager(
                StandaloneResourceManager::new(args.clone()),
            ),
            ClusterMode::YARN => {
                ResourceManager::YarnResourceManager(YarnResourceManager::new(args.clone()))
            }
            ClusterMode::Kubernetes => ResourceManager::KubernetesResourceManager(
                KubernetesResourceManager::new(args.clone()),
            ),
        }
    }
}

#[async_trait]
impl TResourceManager for ResourceManager {
    fn prepare(&mut self, context: &Args, job_descriptor: &ClusterDescriptor) {
        match self {
            ResourceManager::LocalResourceManager(rm) => rm.prepare(context, job_descriptor),
            ResourceManager::StandaloneResourceManager(rm) => rm.prepare(context, job_descriptor),
            ResourceManager::YarnResourceManager(rm) => rm.prepare(context, job_descriptor),
            ResourceManager::KubernetesResourceManager(rm) => rm.prepare(context, job_descriptor),
        }
    }

    async fn worker_allocate(
        &self,
        env: Arc<ApplicationEnv>,
    ) -> anyhow::Result<Vec<TaskResourceInfo>> {
        match self {
            ResourceManager::LocalResourceManager(rm) => rm.worker_allocate(env).await,
            ResourceManager::StandaloneResourceManager(rm) => rm.worker_allocate(env).await,
            ResourceManager::YarnResourceManager(rm) => rm.worker_allocate(env).await,
            ResourceManager::KubernetesResourceManager(rm) => rm.worker_allocate(env).await,
        }
    }

    async fn stop_workers(&self, task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        match self {
            ResourceManager::LocalResourceManager(rm) => rm.stop_workers(task_ids).await,
            ResourceManager::StandaloneResourceManager(rm) => rm.stop_workers(task_ids).await,
            ResourceManager::YarnResourceManager(rm) => rm.stop_workers(task_ids).await,
            ResourceManager::KubernetesResourceManager(rm) => rm.stop_workers(task_ids).await,
        }
    }
}
