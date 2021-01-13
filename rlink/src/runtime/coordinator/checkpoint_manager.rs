use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::api::checkpoint::Checkpoint;
use crate::api::properties::SystemProperties;
use crate::api::runtime::{CheckpointId, JobId};
use crate::dag::DagManager;
use crate::runtime::context::Context;
use crate::runtime::ApplicationDescriptor;
use crate::storage::checkpoint::{CheckpointStorage, CheckpointStorageWrap};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ApplicationCheckpoint {
    application_name: String,
    application_id: String,

    job_id: JobId,
    parallelism: u32,

    #[serde(skip_serializing, skip_deserializing)]
    storage: Option<CheckpointStorageWrap>,

    current_ck_id: CheckpointId,
    /// Map<task_num, Checkpoint>
    current_cks: HashMap<u16, Checkpoint>,
    latest_finish_cks: Vec<Checkpoint>,
}

impl ApplicationCheckpoint {
    pub fn new(
        application_name: String,
        application_id: String,
        job_id: JobId,
        parallelism: u32,
        storage: Option<CheckpointStorageWrap>,
    ) -> Self {
        ApplicationCheckpoint {
            application_name,
            application_id,
            job_id,
            parallelism,
            storage,
            current_ck_id: CheckpointId::default(),
            current_cks: HashMap::with_capacity(parallelism as usize),
            latest_finish_cks: Vec::with_capacity(parallelism as usize),
        }
    }

    pub fn add(&mut self, ck: Checkpoint) -> anyhow::Result<()> {
        if ck.checkpoint_id.0 == self.current_ck_id.0 {
            if self.is_align() {
                Err(anyhow::Error::msg(format!(
                    "the Checkpoint has align. {:?}",
                    &ck
                )))
            } else if self.current_cks.contains_key(&ck.task_num) {
                Err(anyhow::Error::msg(format!(
                    "the Checkpoint has existed. {:?}",
                    &ck
                )))
            } else {
                self.current_ck_id = ck.checkpoint_id;
                self.current_cks.insert(ck.task_num, ck);

                self.archive_align();

                Ok(())
            }
        } else if ck.checkpoint_id.0 > self.current_ck_id.0 {
            if self.current_ck_id.0 != 0 && !self.is_align() {
                warn!("not all checkpoint is arrived");
            }

            self.current_ck_id = ck.checkpoint_id;

            self.current_cks.clear();
            self.current_cks.insert(ck.task_num, ck);

            self.archive_align();

            Ok(())
        } else {
            Err(anyhow::Error::msg(format!(
                "checkpoint_id={:?} late. current checkpoint_id={:?}",
                ck.checkpoint_id, self.current_ck_id
            )))
        }
    }

    fn is_align(&self) -> bool {
        self.current_cks.len() == self.parallelism as usize
    }

    fn archive_align(&mut self) {
        if self.is_align() {
            let finish_cks: Vec<Checkpoint> =
                self.current_cks.iter().map(|x| x.1.clone()).collect();
            self.latest_finish_cks.clear();
            self.latest_finish_cks
                .extend_from_slice(finish_cks.as_slice());

            self.storage_checkpoint(finish_cks);
        }
    }

    fn storage_checkpoint(&mut self, cks: Vec<Checkpoint>) {
        match self.storage.as_mut() {
            Some(storage) => {
                let rt = storage.save(
                    self.application_name.as_str(),
                    self.application_id.as_str(),
                    self.job_id,
                    self.current_ck_id,
                    cks,
                    Duration::from_secs(60 * 30).as_millis() as u64,
                );
                match rt {
                    Ok(_) => {}
                    Err(e) => error!("checkpoint storage error. {}", e),
                }
            }
            None => {}
        }
    }

    pub fn load(&mut self) -> anyhow::Result<Vec<Checkpoint>> {
        match self.storage.as_mut() {
            Some(storage) => storage.load(self.application_name.as_str(), self.job_id),
            None => Ok(vec![]),
        }
    }
}

impl Clone for ApplicationCheckpoint {
    fn clone(&self) -> Self {
        ApplicationCheckpoint {
            application_name: self.application_name.clone(),
            application_id: self.application_id.clone(),
            job_id: self.job_id,
            parallelism: self.parallelism,
            storage: None,
            current_ck_id: self.current_ck_id,
            current_cks: self.current_cks.clone(),
            latest_finish_cks: self.latest_finish_cks.clone(),
        }
    }
}

pub(crate) type JobCheckpointSafe = Arc<RwLock<ApplicationCheckpoint>>;

#[derive(Debug)]
pub(crate) struct CheckpointManager {
    application_name: String,
    job_cks: dashmap::DashMap<JobId, JobCheckpointSafe>,
}

impl CheckpointManager {
    pub fn new(
        dag_manager: &DagManager,
        context: &Context,
        application_descriptor: &ApplicationDescriptor,
    ) -> Self {
        let checkpoint_backend = application_descriptor
            .coordinator_manager
            .application_properties
            .get_checkpoint()
            .map(|x| Some(x))
            .unwrap_or(None);

        let job_cks = dashmap::DashMap::new();
        for job_node in dag_manager.job_graph().get_nodes() {
            let application_name = context.application_name.clone();
            let application_id = context.application_id.clone();
            let job_id = job_node.job_id;
            let parallelism = job_node.parallelism;
            let storage = checkpoint_backend
                .as_ref()
                .map(|ck_backend| CheckpointStorageWrap::new(ck_backend));
            let job_ck = ApplicationCheckpoint::new(
                application_name,
                application_id,
                job_id,
                parallelism,
                storage,
            );

            job_cks.insert(job_id, Arc::new(RwLock::new(job_ck)));
        }

        CheckpointManager {
            application_name: context.application_name.clone(),
            job_cks,
        }
    }

    pub fn add(&self, ck: Checkpoint) -> anyhow::Result<()> {
        match self.job_cks.get_mut(&ck.job_id) {
            Some(mut d) => {
                let mut job_checkpoint = d.value_mut().write().unwrap();
                job_checkpoint.add(ck)
            }
            None => Err(anyhow::Error::msg(format!(
                "checkpoint_id={:?} not found",
                ck
            ))),
        }
    }

    pub fn load(&mut self) -> anyhow::Result<HashMap<JobId, Vec<Checkpoint>>> {
        let mut job_checkpoints = HashMap::new();
        for entry in &self.job_cks {
            let mut job_ck = entry.value().write().unwrap();
            let checkpoints = job_ck.load()?;
            job_checkpoints.insert(*entry.key(), checkpoints);
        }

        Ok(job_checkpoints)
    }

    pub fn get(&self) -> HashMap<JobId, ApplicationCheckpoint> {
        let mut map = HashMap::new();
        for entry in &self.job_cks {
            let job_ck = entry.value().read().unwrap();
            map.insert(entry.key().clone(), job_ck.clone());
        }

        map
    }
}

impl Clone for CheckpointManager {
    fn clone(&self) -> Self {
        let job_cks = dashmap::DashMap::new();
        for entry in &self.job_cks {
            job_cks.insert(entry.key().clone(), entry.value().clone());
        }

        CheckpointManager {
            application_name: self.application_name.clone(),
            job_cks,
        }
    }
}
