use std::collections::HashMap;
use std::sync::Arc;

use crate::api::cluster::{BatchExecuteRequest, ResponseCode, StdResponse, TaskResourceInfo};
use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::deployment::{Resource, TResourceManager};
use crate::runtime::context::Context;
use crate::runtime::{ClusterDescriptor, ManagerType};
use crate::utils::http;

#[derive(Clone, Debug)]
pub(crate) struct StandaloneResourceManager {
    context: Arc<Context>,
    cluster_descriptor: Option<ClusterDescriptor>,
}

impl StandaloneResourceManager {
    pub fn new(context: Arc<Context>) -> Self {
        StandaloneResourceManager {
            context,
            cluster_descriptor: None,
        }
    }
}

impl TResourceManager for StandaloneResourceManager {
    fn prepare(&mut self, _context: &Context, cluster_descriptor: &ClusterDescriptor) {
        self.cluster_descriptor = Some(cluster_descriptor.clone());
    }

    fn worker_allocate<S>(
        &self,
        _stream_app_clone: &S,
        _stream_env: &StreamExecutionEnvironment,
    ) -> anyhow::Result<Vec<TaskResourceInfo>>
    where
        S: StreamApp + 'static,
    {
        let cluster_descriptor = self.cluster_descriptor.as_ref().unwrap();

        let cluster_client = StandaloneClusterClient::new(
            self.context
                .cluster_config
                .application_manager_address
                .clone(),
        );

        let application_id = self.context.application_id.as_str();
        let mut task_args = Vec::new();
        for task_manager_descriptor in &cluster_descriptor.worker_managers {
            let resource = Resource::new(
                cluster_descriptor.coordinator_manager.memory_mb,
                cluster_descriptor.coordinator_manager.v_cores,
            );

            info!(
                "TaskManager(id={}) allocate resource cpu:{}, memory:{}",
                task_manager_descriptor.task_manager_id, resource.cpu_cores, resource.memory
            );

            let mut args = HashMap::new();
            args.insert(
                "cluster_mode".to_string(),
                self.context.cluster_mode.to_string(),
            );
            args.insert("manager_type".to_string(), ManagerType::Worker.to_string());
            // args.insert(
            //     "application_id".to_string(),
            //     self.context.application_id.clone(),
            // );
            args.insert(
                "task_manager_id".to_string(),
                task_manager_descriptor.task_manager_id.clone(),
            );
            args.insert(
                "coordinator_address".to_string(),
                cluster_descriptor
                    .coordinator_manager
                    .coordinator_address
                    .clone(),
            );

            task_args.push(args);
        }
        cluster_client.allocate_worker(application_id, task_args)
    }

    fn stop_workers(&self, task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        let cluster_client = StandaloneClusterClient::new(
            self.context
                .cluster_config
                .application_manager_address
                .clone(),
        );
        cluster_client.stop_all_workers(self.context.application_id.as_str(), task_ids)
    }
}

struct StandaloneClusterClient {
    pub application_manager_address: Vec<String>,
}

impl StandaloneClusterClient {
    pub fn new(application_manager_address: Vec<String>) -> Self {
        StandaloneClusterClient {
            application_manager_address,
        }
    }

    pub fn allocate_worker(
        &self,
        application_id: &str,
        args: Vec<HashMap<String, String>>,
    ) -> anyhow::Result<Vec<TaskResourceInfo>> {
        for application_manager_address in &self.application_manager_address {
            match self.allocate_worker0(
                application_manager_address.as_str(),
                application_id,
                args.clone(),
            ) {
                Ok(rt) => {
                    return Ok(rt);
                }
                Err(e) => {
                    warn!("try allocate worker resource failure. {}", e);
                }
            }
        }

        Err(anyhow::Error::msg("No available JobManager"))
    }

    pub fn allocate_worker0(
        &self,
        application_manager_address: &str,
        application_id: &str,
        args: Vec<HashMap<String, String>>,
    ) -> Result<Vec<TaskResourceInfo>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{}/job/application/{}",
            application_manager_address, application_id
        );

        info!("allocate worker, url={}, body={:?}", url, &args);

        let request_body = BatchExecuteRequest { batch_args: args };
        let body = serde_json::to_string(&request_body).unwrap();
        let result: StdResponse<String> = http::client::post_sync(url, body)?;

        info!("allocation success. `code`={:?}", result.code);

        let data: Vec<TaskResourceInfo> = serde_json::from_str(result.data.unwrap().as_str())?;
        Ok(data)
    }

    pub fn stop_all_workers(
        &self,
        application_id: &str,
        task_ids: Vec<TaskResourceInfo>,
    ) -> anyhow::Result<()> {
        for application_manager_address in &self.application_manager_address {
            match self.stop_all_workers0(
                application_manager_address.as_str(),
                application_id,
                &task_ids,
            ) {
                Ok(rt) => {
                    return Ok(rt);
                }
                Err(e) => {
                    warn!("try allocate worker resource failure. {}", e);
                }
            }
        }

        Err(anyhow::Error::msg("No available JobManager"))
    }

    pub fn stop_all_workers0(
        &self,
        application_manager_address: &str,
        application_id: &str,
        task_ids: &Vec<TaskResourceInfo>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{}/job/application/{}/task/shutdown",
            application_manager_address, application_id
        );

        let body = serde_json::to_string(&task_ids)?;

        info!("stop all workers, url={}, body={:?}", url, body.as_str());

        let result: StdResponse<String> = http::client::post_sync(url, body)?;

        match &result.code {
            ResponseCode::OK => {
                info!("stop worker success. ");
            }
            ResponseCode::ERR(msg) => {
                error!("stop worker error. {}", msg);
            }
        }

        Ok(())
    }
}
