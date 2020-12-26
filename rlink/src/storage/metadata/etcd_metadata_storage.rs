use crate::runtime::{
    JobDescriptor, JobManagerDescriptor, TaskManagerDescriptor, TaskManagerStatus,
};
use crate::storage::metadata::MetadataStorage;
use crate::utils;
use crate::utils::get_runtime;
use etcd_rs::{
    Client, ClientConfig, DeleteRequest, KeyRange, KeyValue, PutRequest, PutResponse, RangeRequest,
};
use std::convert::TryFrom;
use std::error::Error;
use std::io::ErrorKind;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct EtcdMetadataStorage {
    job_id: String,
    endpoints: Vec<String>,
}

impl EtcdMetadataStorage {
    pub fn new(job_id: String, endpoints: Vec<String>) -> Self {
        EtcdMetadataStorage { job_id, endpoints }
    }
}

impl MetadataStorage for EtcdMetadataStorage {
    fn save_job_descriptor(
        &mut self,
        metadata: JobDescriptor,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let endpoints = self.endpoints.clone();
        let job_id = self.job_id.clone();
        get_runtime().block_on(async move {
            let client = EtcdClient::new(endpoints).await?;
            let key = format!("{}-job_manager", job_id.as_str());
            let val = serde_json::to_string(&metadata.job_manager).unwrap();
            client.put(key, val).await?;

            for task_manager_descriptor in &metadata.task_managers {
                let key = format!(
                    "{}-task_manager-{}",
                    job_id.as_str(),
                    task_manager_descriptor.task_manager_id
                );
                let val = serde_json::to_string(task_manager_descriptor).unwrap();
                client.put(key, val).await?;
            }

            client.shutdown().await?;

            Ok(())
        })
    }

    fn delete_job_descriptor(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let endpoints = self.endpoints.clone();
        let job_id = self.job_id.clone();
        get_runtime().block_on(async move {
            let client = EtcdClient::new(endpoints).await?;
            client.del_prefix(job_id).await?;
            client.shutdown().await?;
            Ok(())
        })
    }

    fn read_job_descriptor(&self) -> Result<JobDescriptor, Box<dyn Error + Send + Sync>> {
        let endpoints = self.endpoints.clone();
        let job_id = self.job_id.clone();
        get_runtime().block_on(async move {
            let client = EtcdClient::new(endpoints).await?;
            let key = format!("{}-job_manager", job_id.as_str());
            let kvs = client.get(key).await?;
            if kvs.is_empty() {
                return Err(Box::try_from(std::io::Error::new(
                    ErrorKind::NotFound,
                    "`JobManager metadata not found",
                ))
                .unwrap());
            }

            let kv = &kvs[0];

            let job_manager = serde_json::from_str(kv.value_str())?;

            let mut task_managers = Vec::new();
            let key_prefix = format!("{}-task_manager-", job_id.as_str());
            let kvs = client.prefix(key_prefix).await?;
            for kv in kvs {
                let task_manager_descriptor = serde_json::from_str(kv.value_str())?;
                task_managers.push(task_manager_descriptor);
            }

            client.shutdown().await?;

            Ok(JobDescriptor {
                job_manager,
                task_managers,
            })
        })
    }

    fn update_job_status(
        &self,
        job_manager_status: TaskManagerStatus,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let endpoints = self.endpoints.clone();
        let job_id = self.job_id.clone();
        get_runtime().block_on(async move {
            let client = EtcdClient::new(endpoints).await?;

            let key = format!("{}-job_manager", job_id.as_str());
            let kvs = client.get(key.clone()).await?;
            // let kv = kvs.get(0).expect("JobManager metadata not found");
            if kvs.is_empty() {
                return Err(Box::try_from(std::io::Error::new(
                    ErrorKind::NotFound,
                    "`JobManager metadata not found",
                ))
                .unwrap());
            }

            let kv = &kvs[0];

            let mut job_manager_descriptor: JobManagerDescriptor =
                serde_json::from_str(kv.value_str())?;
            job_manager_descriptor.job_status = job_manager_status;

            let val = serde_json::to_string(&job_manager_descriptor).unwrap();
            client.put(key, val).await?;
            client.shutdown().await?;

            Ok(())
        })
    }

    fn update_task_status(
        &self,
        task_manager_id: &str,
        task_manager_address: &str,
        task_manager_status: TaskManagerStatus,
        metrics_address: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let endpoints = self.endpoints.clone();
        let job_id = self.job_id.clone();
        get_runtime().block_on(async move {
            let client = EtcdClient::new(endpoints).await?;

            let key = format!("{}-task_manager-{}", job_id.as_str(), task_manager_id);
            let kvs = client.get(key.clone()).await?;
            let kv = kvs.get(0).expect("JobManager metadata not found");

            let mut task_manager_descriptor: TaskManagerDescriptor =
                serde_json::from_str(kv.value_str())?;
            debug!("{:?}", &task_manager_descriptor);

            task_manager_descriptor.task_manager_address = task_manager_address.to_string();
            task_manager_descriptor.task_status = task_manager_status;
            task_manager_descriptor.latest_heart_beat_ts =
                utils::date_time::current_timestamp_millis();
            task_manager_descriptor.metrics_address = metrics_address.to_string();

            let val = serde_json::to_string(&task_manager_descriptor).unwrap();
            client.put(key, val).await?;
            client.shutdown().await?;
            Ok(())
        })
    }
}

pub struct EtcdClient {
    client: Client,
}

impl EtcdClient {
    pub async fn new(
        endpoints: Vec<String>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let client = Client::connect(ClientConfig {
            endpoints: endpoints.clone(),
            auth: None,
        })
        .await?;

        Ok(EtcdClient { client })
    }

    pub async fn put(
        &self,
        key: String,
        val: String,
    ) -> Result<PutResponse, Box<dyn std::error::Error + Send + Sync + 'static>> {
        // Put a key-value pair
        self.client.kv().put(PutRequest::new(key, val)).await
    }

    pub async fn get(
        &self,
        key: String,
    ) -> std::result::Result<Vec<KeyValue>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        debug!("Etcd get {}", &key);

        // Put a key-value pair
        self.client
            .kv()
            .range(RangeRequest::new(KeyRange::key(key)))
            .await
            .map(|mut resp| resp.take_kvs())
    }

    pub async fn prefix(
        &self,
        key: String,
    ) -> std::result::Result<Vec<KeyValue>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        self.client
            .kv()
            .range(RangeRequest::new(KeyRange::prefix(key)))
            .await
            .map(|mut resp| resp.take_kvs())
    }

    pub async fn del_prefix(
        &self,
        key: String,
    ) -> std::result::Result<Vec<KeyValue>, Box<dyn std::error::Error + Send + Sync + 'static>>
    {
        self.client
            .kv()
            .delete(DeleteRequest::new(KeyRange::prefix(key)))
            .await
            .map(|mut resp| resp.take_prev_kvs())
    }

    pub async fn shutdown(
        &self,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.client.shutdown().await
    }
}
