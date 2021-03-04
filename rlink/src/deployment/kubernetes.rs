use crate::api::{
    cluster::TaskResourceInfo,
    env::{StreamApp, StreamExecutionEnvironment},
};
use crate::deployment::TResourceManager;
use crate::runtime::context::Context;
use crate::runtime::{ClusterDescriptor, WorkerManagerDescriptor};
use std::sync::Arc;

use k8s_openapi::api::{apps::v1::Deployment, core::v1::Pod};
use kube::{
    api::{Api, DeleteParams, ListParams, Meta, PostParams},
    config::Config,
    Client,
};
use serde_json::json;
use std::convert::TryFrom;
use std::fmt;

#[derive(Clone, Debug)]
pub(crate) struct KubernetesResourceManager {
    context: Arc<Context>,
    job_descriptor: Option<ClusterDescriptor>,
    k8s_client: Option<K8sClient>,
}

impl KubernetesResourceManager {
    pub fn new(context: Arc<Context>) -> Self {
        KubernetesResourceManager {
            context,
            job_descriptor: None,
            k8s_client: None,
        }
    }
}

impl TResourceManager for KubernetesResourceManager {
    fn prepare(&mut self, context: &Context, job_descriptor: &ClusterDescriptor) {
        self.job_descriptor = Some(job_descriptor.clone());
        self.k8s_client = Some(K8sClient::new(context,
            job_descriptor.coordinator_manager.application_name.clone(),
        ));
    }

    fn worker_allocate<S>(
        &self,
        _stream_app_clone: &S,
        _stream_env: &StreamExecutionEnvironment,
    ) -> anyhow::Result<Vec<TaskResourceInfo>>
    where
        S: StreamApp + 'static,
    {
        let job_descriptor = self.job_descriptor.as_ref().unwrap();
        let mut task_infos = Vec::new();
        let rt = tokio::runtime::Runtime::new()?;
        let k8s_client = self.k8s_client.as_ref().unwrap();
        let job_deploy_id = rt.block_on(async {
            k8s_client
                .get_job_deploy_id(&job_descriptor.coordinator_manager.application_name)
                .await
                .unwrap()
        });

        for task_manager_descriptor in &job_descriptor.worker_managers {
            let task_manager_id = task_manager_descriptor.task_manager_id.clone();
            rt.block_on(async {
              
                match k8s_client
                    .allocate_worker(job_deploy_id.clone(), task_manager_descriptor)
                    .await
                {
                    Ok(o) => {
                        let task_info = TaskResourceInfo::new(task_manager_id, String::new(), o);
                        task_infos.push(task_info);
                    }
                    _ => {
                        error!("worker {} allocate failed", task_manager_id)
                    }
                }
            });
        }
        Ok(task_infos)
    }

    fn stop_workers(&self, task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        let mut tasks: Vec<String> = Vec::new();
        for task in task_ids {
            if let Some(task_id) = task.task_id() {
                tasks.push(format!("name={}", task_id));
            }
            tasks.push(format!("uid={}", task.task_manager_id));
        }
        let k8s_client = self.k8s_client.as_ref().unwrap();
        k8s_client.stop_worker(tasks);
        Ok(())
    }
}

#[derive(Clone)]
struct K8sClient {
    k8s_config: Config,
    client: Client,
    namespace: String,
    cluster_name: String,
    limits: ContainerLimits,
}

#[derive(Clone, Debug)]
struct ContainerLimits {
    cpu: usize,
    memory: String,
}

impl fmt::Debug for K8sClient {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "K8sClient")
    }
}

impl K8sClient {
    fn new(context: &Context, cluster_name: String) -> Self {
        let config = Config::from_cluster_env().unwrap();
        K8sClient {
            k8s_config: config.clone(),
            client: Client::try_from(config).unwrap(),
            namespace: String::from("default"),
            cluster_name,
            limits: ContainerLimits {
                cpu: context.v_cores,
                memory: format!("{}Mi", context.memory_mb),
            },
        }
    }

    async fn allocate_worker(
        &self,
        job_deploy_id: String,
        task_manager_descriptor: &WorkerManagerDescriptor,
    ) -> anyhow::Result<String> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), self.namespace.as_str());
        let p: Pod = serde_json::from_value(json!(
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": { "name": task_manager_descriptor.task_manager_id },
                "ownerReferences":{
                    "kind":"Deployment",
                    "apiVersion": "apps/v1",
                    "name":self.cluster_name,
                    "uid":job_deploy_id,
                    "controller": true,
                    "blockOwnerDeletion": true
                },
                "spec": {
                    "containers": [
                        {
                            "name": task_manager_descriptor.task_manager_id,
                            "image": "",
                            "limits":{
                                "cpu":self.limits.cpu,
                                "memory":self.limits.memory
                            },
                            "args":[
                                "cluster_mode=kubernetes",
                                "manager_type=Worker",
                            ]
                        }
                    ]
                }
            }
        ))?;

        let pp = PostParams::default();
        let mut uid = String::new();
        match pods.create(&pp, &p).await {
            Ok(o) => {
                uid = Meta::meta(&o).uid.clone().expect("kind has metadata.uid");
                // wait for it..
            }
            Err(kube::Error::Api(ae)) => assert_eq!(ae.code, 409), // if you skipped delete, for instance
            Err(e) => return Err(e.into()),                        // any other case is probably bad
        }
        Ok(uid)
    }

    fn stop_worker(&self, task_ids: Vec<String>) {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), self.namespace.as_str());

        let dp = DeleteParams::default();
        let mut lp = ListParams::default();
        for task_id in task_ids {
            lp = lp.fields(task_id.as_str());
        }
        pods.delete_collection(&dp, &lp);
    }

    async fn get_job_deploy_id(&self, cluster_name: &str) -> anyhow::Result<String> {
        let deployment: Api<Deployment> =
            Api::namespaced(self.client.clone(), self.namespace.as_str());

        let mut uid = String::new();

        match deployment.get(cluster_name).await {
            Ok(d) => {
                if let Some(id) = d.metadata.uid {
                    uid = id
                }
            }
            _ => {}
        }

        Ok(uid)
    }
}
