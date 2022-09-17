use std::sync::Arc;

use crate::core::cluster::TaskResourceInfo;
use crate::core::env::StreamApp;
use crate::core::runtime::ClusterDescriptor;
#[cfg(feature = "k8s")]
use crate::deployment::kubernetes::KubernetesResourceManager;
use crate::deployment::local::LocalResourceManager;
use crate::deployment::standalone::StandaloneResourceManager;
use crate::deployment::yarn::YarnResourceManager;
use crate::runtime::context::Context;
use crate::runtime::ClusterMode;

#[cfg(feature = "k8s")]
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
    fn prepare(&mut self, context: &Context, job_descriptor: &ClusterDescriptor);

    /// worker resource allocate
    /// Return a resource location.
    async fn worker_allocate<S>(&self, stream_app: &S) -> anyhow::Result<Vec<TaskResourceInfo>>
    where
        S: StreamApp + 'static;

    async fn stop_workers(&self, task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()>;
}

pub(crate) enum ResourceManager {
    LocalResourceManager(LocalResourceManager),
    StandaloneResourceManager(StandaloneResourceManager),
    YarnResourceManager(YarnResourceManager),
    #[cfg(feature = "k8s")]
    KubernetesResourceManager(KubernetesResourceManager),
}

impl ResourceManager {
    pub fn new(context: Arc<Context>) -> Self {
        match context.cluster_mode {
            ClusterMode::Local => {
                ResourceManager::LocalResourceManager(LocalResourceManager::new(context.clone()))
            }
            ClusterMode::Standalone => ResourceManager::StandaloneResourceManager(
                StandaloneResourceManager::new(context.clone()),
            ),
            ClusterMode::YARN => {
                ResourceManager::YarnResourceManager(YarnResourceManager::new(context.clone()))
            }
            #[cfg(feature = "k8s")]
            ClusterMode::Kubernetes => ResourceManager::KubernetesResourceManager(
                KubernetesResourceManager::new(context.clone()),
            ),
            #[cfg(not(feature = "k8s"))]
            ClusterMode::Kubernetes => unimplemented!(),
        }
    }
}

#[async_trait]
impl TResourceManager for ResourceManager {
    fn prepare(&mut self, context: &Context, job_descriptor: &ClusterDescriptor) {
        match self {
            ResourceManager::LocalResourceManager(rm) => rm.prepare(context, job_descriptor),
            ResourceManager::StandaloneResourceManager(rm) => rm.prepare(context, job_descriptor),
            ResourceManager::YarnResourceManager(rm) => rm.prepare(context, job_descriptor),
            #[cfg(feature = "k8s")]
            ResourceManager::KubernetesResourceManager(rm) => rm.prepare(context, job_descriptor),
        }
    }

    async fn worker_allocate<S>(&self, stream_app: &S) -> anyhow::Result<Vec<TaskResourceInfo>>
    where
        S: StreamApp + 'static,
    {
        match self {
            ResourceManager::LocalResourceManager(rm) => rm.worker_allocate(stream_app).await,
            ResourceManager::StandaloneResourceManager(rm) => rm.worker_allocate(stream_app).await,
            ResourceManager::YarnResourceManager(rm) => rm.worker_allocate(stream_app).await,
            #[cfg(feature = "k8s")]
            ResourceManager::KubernetesResourceManager(rm) => rm.worker_allocate(stream_app).await,
        }
    }

    async fn stop_workers(&self, task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        match self {
            ResourceManager::LocalResourceManager(rm) => rm.stop_workers(task_ids).await,
            ResourceManager::StandaloneResourceManager(rm) => rm.stop_workers(task_ids).await,
            ResourceManager::YarnResourceManager(rm) => rm.stop_workers(task_ids).await,
            #[cfg(feature = "k8s")]
            ResourceManager::KubernetesResourceManager(rm) => rm.stop_workers(task_ids).await,
        }
    }
}
