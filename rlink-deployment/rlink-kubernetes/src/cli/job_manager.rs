use crate::config::k8s_config::JobConfig;
use k8s_openapi::{api::apps::v1::Deployment};
use kube::{
    api::{Api, Meta, PostParams},
    Client,
};
use serde_json::json;
use std::convert::TryFrom;

pub async fn run(cfg: JobConfig, cluster_name: String) -> anyhow::Result<String> {

    //todo check cluster_name unique

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
                    "cpu":cfg.cpu,
                    "memory":cfg.memory
              },
              "args":[
                  "cluster_mode=kubernetes",
                  "manager_type=Coordinator",
                  "num_task_managers=1"
                ]
            }],
        }
    }))?;

    let pp = PostParams::default();

    match deployment.create(&pp, &d).await {
        Ok(o) => {
            // let name = Meta::name(&o);
            let uid = Meta::meta(&o).uid.clone().expect("kind has metadata.uid");
            return Ok(uid)
        }
        Err(kube::Error::Api(ae)) => assert_eq!(ae.code, 409), // if you skipped delete, for instance
        Err(e) => return Err(e.into()),                        // any other case is probably bad
    }
    Ok(String::default())
}
