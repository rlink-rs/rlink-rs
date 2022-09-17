use std::sync::Arc;

use k8s_openapi::api::{apps::v1::Deployment, core::v1::Pod};
use kube::{
    api::{Api, DeleteParams, ListParams, PostParams},
    Client,
};
use serde_json::json;

use crate::core::runtime::ClusterDescriptor;
use crate::core::{
    cluster::TaskResourceInfo,
    env::{StreamApp, StreamExecutionEnvironment},
};
use crate::deployment::TResourceManager;
use crate::runtime::context::Context;
use crate::runtime::ClusterDescriptor;

#[derive(Clone)]
pub(crate) struct KubernetesResourceManager {
    context: Arc<Context>,
    cluster_descriptor: Option<ClusterDescriptor>,
}

impl KubernetesResourceManager {
    pub fn new(context: Arc<Context>) -> Self {
        KubernetesResourceManager {
            context,
            cluster_descriptor: None,
        }
    }
}

#[async_trait]
impl TResourceManager for KubernetesResourceManager {
    fn prepare(&mut self, _context: &Context, job_descriptor: &ClusterDescriptor) {
        self.cluster_descriptor = Some(job_descriptor.clone());
    }

    async fn worker_allocate<S>(
        &self,
        _stream_app_clone: &S,
    ) -> anyhow::Result<Vec<TaskResourceInfo>>
    where
        S: StreamApp + 'static,
    {
        let cluster_descriptor = self.cluster_descriptor.as_ref().unwrap();
        let coordinator_manager = &cluster_descriptor.coordinator_manager;

        let mut task_infos = Vec::new();
        let namespace = "default";
        let image_path = &self.context.image_path;
        let limits = &ContainerLimits {
            cpu: coordinator_manager.v_cores as usize,
            memory: format!("{}Mi", coordinator_manager.memory_mb),
        };

        let application_id = coordinator_manager.application_id.as_str();
        let job_deploy_id = get_job_deploy_id(namespace, application_id).await.unwrap();

        let coordinator_address = coordinator_manager.coordinator_address.as_str();

        for task_manager_descriptor in &cluster_descriptor.worker_managers {
            let task_manager_id = task_manager_descriptor.task_manager_id.clone();
            let task_manager_name = format!(
                "{}-{}",
                application_id,
                parse_name(task_manager_id.as_str())
            );
            let pod_uid = allocate_worker(
                coordinator_address,
                task_manager_id.as_str(),
                task_manager_name.as_str(),
                application_id,
                namespace,
                job_deploy_id.as_str(),
                image_path,
                limits,
            )
            .await;
            match pod_uid {
                Ok(pod_uid) => {
                    let mut task_info = TaskResourceInfo::new(
                        pod_uid.clone(),
                        String::new(),
                        task_manager_id.clone(),
                    );
                    task_info
                        .resource_info
                        .insert("task_manager_name".to_string(), task_manager_name);
                    task_infos.push(task_info);
                    info!(
                        "worker id :{}, task_manager_id {} allocate success",
                        task_manager_id.clone(),
                        pod_uid
                    );
                }
                _ => {
                    error!("worker {} allocate failed", task_manager_id)
                }
            }
        }
        Ok(task_infos)
    }

    async fn stop_workers(&self, task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        let mut tasks: Vec<String> = Vec::new();
        for task in task_ids {
            if let Some(task_id) = task.task_id() {
                tasks.push(format!("uid={}", task_id));
            }
            tasks.push(format!("name={}", task.resource_info["task_manager_name"]));
        }

        let namespace = "default";
        stop_worker(namespace, tasks).await
    }
}

#[derive(Clone, Debug)]
struct ContainerLimits {
    cpu: usize,
    memory: String,
}

async fn allocate_worker(
    coordinator_address: &str,
    task_manager_id: &str,
    task_manager_name: &str,
    cluster_name: &str,
    namespace: &str,
    job_deploy_id: &str,
    image_path: &str,
    limits: &ContainerLimits,
) -> anyhow::Result<String> {
    let client = Client::try_default().await?;
    let pods: Api<Pod> = Api::namespaced(client, namespace);
    let p: Pod = serde_json::from_value(json!(
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": task_manager_name,
                "labels":{
                    "app":"rlink",
                    "commpent":"jobmanager",
                    "type":"rlinl-on-k8s"
                },
                "ownerReferences":[{
                    "kind":"Deployment",
                    "apiVersion": "apps/v1",
                    "name":cluster_name,
                    "uid":job_deploy_id,
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {
                "containers": [
                    {
                        "name":task_manager_name,
                        "image": image_path,
                        "limits":{
                            "cpu":limits.cpu,
                            "memory":limits.memory
                        },
                        "args":[
                            "cluster_mode=kubernetes",
                            "manager_type=Worker",
                            format!("application_id={}",cluster_name),
                            format!("task_manager_id={}",task_manager_id),
                            format!("coordinator_address={}",coordinator_address),
                        ]
                    }
                ],
                "restartPolicy":"OnFailure"
            }
        }
    ))?;

    let pp = PostParams::default();
    let mut uid = String::new();
    match pods.create(&pp, &p).await {
        Ok(pod) => {
            info!("create worker({})pod success", task_manager_name);
            // uid = Meta::meta(&pod).uid.clone().expect("kind has metadata.uid");
            uid = pod.metadata.uid.expect("kind has metadata.uid").to_string();
            // wait for it..
        }
        Err(kube::Error::Api(ae)) => {
            error!("{:?}", ae);
            assert_eq!(ae.code, 409)
        } // if you skipped delete, for instance
        Err(e) => return Err(e.into()), // any other case is probably bad
    }
    Ok(uid)
}

async fn stop_worker(namespace: &str, task_ids: Vec<String>) -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let pods: Api<Pod> = Api::namespaced(client, namespace);
    let dp = DeleteParams::default();
    let mut lp = ListParams::default();
    for task_id in task_ids {
        lp = lp.fields(task_id.as_str());
    }
    match pods.delete_collection(&dp, &lp).await {
        Ok(_o) => info!("stop worker success"),
        Err(e) => error!("stop worker failed. {:?}", e),
    };
    Ok(())
}

async fn get_job_deploy_id(namespace: &str, cluster_name: &str) -> anyhow::Result<String> {
    info!(
        "get application {} deploy id on namespace :{}",
        cluster_name, namespace
    );
    let client = Client::try_default().await?;
    let deployment: Api<Deployment> = Api::namespaced(client, namespace);
    let mut uid = String::new();
    match deployment.get(cluster_name).await {
        Ok(d) => {
            if let Some(id) = d.metadata.uid {
                info!(
                    "get application {} deploy id on namespace {} success:{}",
                    cluster_name, namespace, id
                );
                uid = id;
            }
        }
        _ => {}
    }
    Ok(uid)
}

fn parse_name(name: &str) -> String {
    return name.replace("_", "-");
}
