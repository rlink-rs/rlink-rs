use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::api::checkpoint::Checkpoint;
use crate::api::properties::SystemProperties;
use crate::dag::DagManager;
use crate::runtime::context::Context;
use crate::runtime::{ApplicationDescriptor, ChainId};
use crate::storage::checkpoint::{CheckpointStorage, CheckpointStorageWrap};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct JobCheckpoint {
    job_name: String,
    job_id: String,

    chain_id: ChainId,
    parallelism: u32,

    #[serde(skip_serializing, skip_deserializing)]
    storage: Option<CheckpointStorageWrap>,

    current_ck_id: u64,
    /// Map<task_num, Checkpoint>
    current_cks: HashMap<u16, Checkpoint>,
    latest_finish_cks: Vec<Checkpoint>,
}

impl JobCheckpoint {
    pub fn new(
        job_name: String,
        job_id: String,
        chain_id: u32,
        parallelism: u32,
        storage: Option<CheckpointStorageWrap>,
    ) -> Self {
        JobCheckpoint {
            job_name,
            job_id,
            chain_id,
            parallelism,
            storage,
            current_ck_id: 0,
            current_cks: HashMap::with_capacity(parallelism as usize),
            latest_finish_cks: Vec::with_capacity(parallelism as usize),
        }
    }

    pub fn add(&mut self, ck: Checkpoint) -> anyhow::Result<()> {
        if ck.checkpoint_id == self.current_ck_id {
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
        } else if ck.checkpoint_id > self.current_ck_id {
            if self.current_ck_id != 0 && !self.is_align() {
                warn!("not all checkpoint is arrived");
            }

            self.current_ck_id = ck.checkpoint_id;

            self.current_cks.clear();
            self.current_cks.insert(ck.task_num, ck);

            self.archive_align();

            Ok(())
        } else {
            Err(anyhow::Error::msg(format!(
                "checkpoint_id={} late. current checkpoint_id={}",
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

            // todo save checkpoint
            self.storage_checkpoint(finish_cks);
        }
    }

    fn storage_checkpoint(&mut self, cks: Vec<Checkpoint>) {
        match self.storage.as_mut() {
            Some(storage) => {
                let rt = storage.save(
                    self.job_name.as_str(),
                    self.job_id.as_str(),
                    self.chain_id,
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
            Some(storage) => storage.load(self.job_name.as_str(), self.chain_id),
            None => Ok(vec![]),
        }
    }
}

impl Clone for JobCheckpoint {
    fn clone(&self) -> Self {
        JobCheckpoint {
            job_name: self.job_name.clone(),
            job_id: self.job_id.clone(),
            chain_id: self.chain_id,
            parallelism: self.parallelism,
            storage: None,
            current_ck_id: self.current_ck_id,
            current_cks: self.current_cks.clone(),
            latest_finish_cks: self.latest_finish_cks.clone(),
        }
    }
}

pub(crate) type JobCheckpointSafe = Arc<RwLock<JobCheckpoint>>;

#[derive(Debug)]
pub(crate) struct CheckpointManager {
    application_name: String,
    job_cks: dashmap::DashMap<ChainId, JobCheckpointSafe>,
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
            let application_name = context.job_name.clone();
            let application_id = context.job_id.clone();
            let job_id = job_node.job_id;
            let parallelism = job_node.parallelism;
            let storage = checkpoint_backend
                .as_ref()
                .map(|ck_backend| CheckpointStorageWrap::new(ck_backend));
            let chain_ck = JobCheckpoint::new(
                application_name,
                application_id,
                job_id,
                parallelism,
                storage,
            );

            job_cks.insert(job_id, Arc::new(RwLock::new(chain_ck)));
        }

        CheckpointManager {
            application_name: context.job_name.clone(),
            job_cks: job_cks,
        }
    }

    pub fn add(&self, ck: Checkpoint) -> anyhow::Result<()> {
        match self.job_cks.get_mut(&ck.job_id) {
            Some(mut d) => {
                let mut chain_checkpoint = d.value_mut().write().unwrap();
                chain_checkpoint.add(ck)
            }
            None => Err(anyhow::Error::msg(format!(
                "ChainId={} not found",
                ck.checkpoint_id
            ))),
        }
    }

    pub fn load(&mut self) -> anyhow::Result<HashMap<ChainId, Vec<Checkpoint>>> {
        let mut job_checkpoints = HashMap::new();
        for entry in &self.job_cks {
            let mut chain_ck = entry.value().write().unwrap();
            let checkpoints = chain_ck.load()?;
            job_checkpoints.insert(*entry.key(), checkpoints);
        }

        Ok(job_checkpoints)
    }

    pub fn get(&self) -> HashMap<ChainId, JobCheckpoint> {
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
