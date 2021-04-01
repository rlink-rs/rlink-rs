use crate::config::k8s_config::Config;
use k8s_openapi::api::apps::v1::Deployment;
use kube::{
    api::{Api,  PostParams},
    Client,
};
use serde_json::json;

pub async fn run(cfg: Config) -> anyhow::Result<()> {
    let client = Client::try_default().await?;

    let namespace = std::env::var("NAMESPACE").unwrap_or(cfg.namespace.clone().into());
    let deployment: Api<Deployment> = Api::namespaced(client.clone(), &namespace);

    let d: Deployment = serde_json::from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": cfg.cluster_name.clone(),
            "namespace":cfg.namespace.clone(),
            "labels":{
                "app":"rlink",
                "commpent":"jobmanager",
                "type":"rlinl-on-k8s"
            }
        },
        "spec": {
            "selector":{
                "matchLabels":{
                        "app":"rlink"
                }
            },
            "template":{
                "metadata":{
                    "labels":{
                        "app":"rlink",
                        "commpent":"jobmanager",
                        "type":"rlinl-on-k8s"
                   }
                } ,
                "spec":{
                    "containers": [
                        {
                            "name":"jobmanager",
                            "image": cfg.image_path,
                            "limits":{
                                    "cpu":cfg.job_v_cores,
                                    "memory": format!("{}Mi",cfg.job_memory_mb)
                            },
                            "args":[
                                format!("image_path={}",cfg.image_path),
                                format!("application_id={}",cfg.cluster_name.clone()),
                                "cluster_mode=kubernetes",
                                "manager_type=Coordinator",
                                format!("num_task_managers={}",cfg.num_task_managers),
                                format!("v_cores={}",cfg.task_v_cores),
                                format!("memory_mb={}",cfg.task_memory_mb)
                                ]
                        }
                    ]
                }
            }
        }
    }))?;

    let pp = PostParams::default();

    match deployment.create(&pp, &d).await {
        Ok(_o) => {}
        Err(kube::Error::Api(ae)) => {
            assert_eq!(ae.code, 409);
        }
        Err(e) => return Err(e.into()), // any other case is probably bad
    }
    Ok(())
}
