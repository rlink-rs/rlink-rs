use std::fmt::Debug;
use std::sync::Arc;

use crate::api::cluster::TaskResourceInfo;
use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::deployment::local::LocalResourceManager;
use crate::deployment::standalone::StandaloneResourceManager;
use crate::deployment::yarn::YarnResourceManager;
use crate::deployment::kubernetes::KubernetesResourceManager;
use crate::runtime::context::Context;
use crate::runtime::{ClusterDescriptor, ClusterMode};

pub mod local;
pub mod standalone;
pub mod yarn;
pub mod kubernetes;

pub struct Resource {
    memory: u32,
    cpu_cores: u32,
}

impl Resource {
    pub fn new(memory: u32, cpu_cores: u32) -> Self {
        Resource { memory, cpu_cores }
    }
}

pub(crate) trait TResourceManager
where
    Self: Debug,
{
    fn prepare(&mut self, context: &Context, job_descriptor: &ClusterDescriptor);

    /// worker resource allocate
    /// Return a resource location.
    fn worker_allocate<S>(
        &self,
        stream_app: &S,
        stream_env: &StreamExecutionEnvironment,
    ) -> anyhow::Result<Vec<TaskResourceInfo>>
    where
        S: StreamApp + 'static;

    fn stop_workers(&self, task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()>;
}

#[derive(Debug)]
pub(crate) enum ResourceManager {
    LocalResourceManager(LocalResourceManager),
    StandaloneResourceManager(StandaloneResourceManager),
    YarnResourceManager(YarnResourceManager),
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
            },
            ClusterMode::Kubernetes=>{
                ResourceManager::KubernetesResourceManager(KubernetesResourceManager::new(context.clone()))
            }
        }
    }
}

impl TResourceManager for ResourceManager {
    fn prepare(&mut self, context: &Context, job_descriptor: &ClusterDescriptor) {
        match self {
            ResourceManager::LocalResourceManager(rm) => rm.prepare(context, job_descriptor),
            ResourceManager::StandaloneResourceManager(rm) => rm.prepare(context, job_descriptor),
            ResourceManager::YarnResourceManager(rm) => rm.prepare(context, job_descriptor),
            ResourceManager::KubernetesResourceManager(rm)=>rm.prepare(context,job_descriptor),
        }
    }

    fn worker_allocate<S>(
        &self,
        stream_app: &S,
        stream_env: &StreamExecutionEnvironment,
    ) -> anyhow::Result<Vec<TaskResourceInfo>>
    where
        S: StreamApp + 'static,
    {
        match self {
            ResourceManager::LocalResourceManager(rm) => rm.worker_allocate(stream_app, stream_env),
            ResourceManager::StandaloneResourceManager(rm) => {
                rm.worker_allocate(stream_app, stream_env)
            }
            ResourceManager::YarnResourceManager(rm) => rm.worker_allocate(stream_app, stream_env),
            ResourceManager::KubernetesResourceManager(rm)=>rm.worker_allocate(stream_app,stream_env),
        }
    }

    fn stop_workers(&self, task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        match self {
            ResourceManager::LocalResourceManager(rm) => rm.stop_workers(task_ids),
            ResourceManager::StandaloneResourceManager(rm) => rm.stop_workers(task_ids),
            ResourceManager::YarnResourceManager(rm) => rm.stop_workers(task_ids),
            ResourceManager::KubernetesResourceManager(rm)=>rm.stop_workers(task_ids),
        }
    }
}
