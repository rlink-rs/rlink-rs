use crate::config::k8s_config::Config;
use k8s_openapi::api::apps::v1::Deployment;
use kube::{
    api::{Api, Meta, PostParams},
    Client,
};
use serde_json::json;
use std::convert::TryFrom;

pub async fn run(cfg: Config, cluster_name: String) -> anyhow::Result<String> {
    let config = Client::try_default().await?;
    let client = Client::try_from(config).unwrap();

    let namespace = std::env::var("NAMESPACE").unwrap_or(cfg.namespace.clone().into());
    let deployment: Api<Deployment> = Api::namespaced(client.clone(), &namespace);

    let d: Deployment = serde_json::from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": cluster_name,
            "namespace":cfg.namespace.clone()
        },
        "spec": {
            "containers": [{
              "name":"jobmanager",
              "image": cluster_name,
              "limits":{
                    "cpu":cfg.job_v_cores,
                    "memory": format!("{}Mi",cfg.job_memory_mb)
              },
              "args":[
                  "cluster_mode=kubernetes",
                  "manager_type=Coordinator",
                  format!("num_task_managers={}",cfg.num_task_managers),
                  format!("v_cores={}",cfg.task_v_cores),
                  format!("memory_mb={}",cfg.task_memory_mb)
                ]
            }],
        }
    }))?;

    let pp = PostParams::default();

    match deployment.create(&pp, &d).await {
        Ok(o) => {
            // let name = Meta::name(&o);
            let uid = Meta::meta(&o).uid.clone().expect("kind has metadata.uid");
            return Ok(uid);
        }
        Err(kube::Error::Api(ae)) => assert_eq!(ae.code, 409), // if you skipped delete, for instance
        Err(e) => return Err(e.into()),                        // any other case is probably bad
    }
    Ok(String::default())
}
