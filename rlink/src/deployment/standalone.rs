use std::collections::HashMap;

use crate::api::cluster::{BatchExecuteRequest, ResponseCode, StdResponse, TaskResourceInfo};
use crate::api::env::{StreamExecutionEnvironment, StreamJob};
use crate::deployment::{Resource, ResourceManager};
use crate::runtime::context::Context;
use crate::runtime::{ApplicationDescriptor, ManagerType};
use crate::utils::http_client;

#[derive(Clone, Debug)]
pub(crate) struct StandaloneResourceManager {
    context: Context,
    job_descriptor: Option<ApplicationDescriptor>,
}

impl StandaloneResourceManager {
    pub fn new(context: Context) -> Self {
        StandaloneResourceManager {
            context,
            job_descriptor: None,
        }
    }
}

impl ResourceManager for StandaloneResourceManager {
    fn prepare(&mut self, _context: &Context, job_descriptor: &ApplicationDescriptor) {
        self.job_descriptor = Some(job_descriptor.clone());
    }

    fn worker_allocate<S>(
        &self,
        _stream_job_clone: &S,
        _stream_env: &StreamExecutionEnvironment,
    ) -> anyhow::Result<Vec<TaskResourceInfo>>
    where
        S: StreamJob + 'static,
    {
        let job_descriptor = self.job_descriptor.as_ref().unwrap();

        let cluster_client =
            StandaloneClusterClient::new(self.context.cluster_config.job_manager_address.clone());

        let job_id = self.context.job_id.as_str();
        let mut task_args = Vec::new();
        for task_manager_descriptor in &job_descriptor.worker_managers {
            let resource = Resource::new(
                task_manager_descriptor.physical_memory,
                task_manager_descriptor.cpu_cores,
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
            args.insert("job_id".to_string(), self.context.job_id.clone());
            args.insert(
                "task_manager_id".to_string(),
                task_manager_descriptor.task_manager_id.clone(),
            );
            args.insert(
                "coordinator_address".to_string(),
                job_descriptor
                    .coordinator_manager
                    .coordinator_address
                    .clone(),
            );

            task_args.push(args);
        }
        cluster_client.allocate_worker(job_id, task_args)
    }

    fn stop_workers(&self, task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        let cluster_client =
            StandaloneClusterClient::new(self.context.cluster_config.job_manager_address.clone());
        cluster_client.stop_all_workers(self.context.job_id.as_str(), task_ids)
    }
}

struct StandaloneClusterClient {
    pub job_manager_address: Vec<String>,
}

impl StandaloneClusterClient {
    pub fn new(job_manager_address: Vec<String>) -> Self {
        StandaloneClusterClient {
            job_manager_address,
        }
    }

    pub fn allocate_worker(
        &self,
        job_id: &str,
        args: Vec<HashMap<String, String>>,
    ) -> anyhow::Result<Vec<TaskResourceInfo>> {
        for job_manager_address in &self.job_manager_address {
            match self.allocate_worker0(job_manager_address.as_str(), job_id, args.clone()) {
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
        job_manager_address: &str,
        job_id: &str,
        args: Vec<HashMap<String, String>>,
    ) -> Result<Vec<TaskResourceInfo>, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("{}/job/application/{}", job_manager_address, job_id);

        info!("allocate worker, url={}, body={:?}", url, &args);

        let request_body = BatchExecuteRequest { batch_args: args };
        let body = serde_json::to_string(&request_body).unwrap();
        let result: StdResponse<String> = http_client::post_sync(url, body)?;

        info!("allocation success. `code`={:?}", result.code);

        let data: Vec<TaskResourceInfo> = serde_json::from_str(result.data.unwrap().as_str())?;
        Ok(data)
    }

    pub fn stop_all_workers(
        &self,
        job_id: &str,
        task_ids: Vec<TaskResourceInfo>,
    ) -> anyhow::Result<()> {
        for job_manager_address in &self.job_manager_address {
            match self.stop_all_workers0(job_manager_address.as_str(), job_id, &task_ids) {
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
        job_manager_address: &str,
        job_id: &str,
        task_ids: &Vec<TaskResourceInfo>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{}/job/application/{}/task/shutdown",
            job_manager_address, job_id
        );

        let body = serde_json::to_string(&task_ids)?;

        info!("stop all workers, url={}, body={:?}", url, body.as_str());

        let result: StdResponse<String> = http_client::post_sync(url, body)?;

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
