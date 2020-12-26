use crate::api::checkpoint::Checkpoint;
use crate::api::properties::SystemProperties;
use crate::graph::JobGraph;
use crate::runtime::context::Context;
use crate::runtime::{ChainId, JobDescriptor};
use crate::storage::checkpoint::{CheckpointStorage, CheckpointStorageWrap};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ChainCheckpoint {
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

impl ChainCheckpoint {
    pub fn new(
        job_name: String,
        job_id: String,
        chain_id: u32,
        parallelism: u32,
        storage: Option<CheckpointStorageWrap>,
    ) -> Self {
        ChainCheckpoint {
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

impl Clone for ChainCheckpoint {
    fn clone(&self) -> Self {
        ChainCheckpoint {
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

pub(crate) type ChainCheckpointSafe = Arc<RwLock<ChainCheckpoint>>;

#[derive(Debug)]
pub(crate) struct CheckpointManager {
    job_name: String,
    chain_cks: dashmap::DashMap<ChainId, ChainCheckpointSafe>,
}

impl CheckpointManager {
    pub fn new(job_graph: &JobGraph, context: &Context, job_descriptor: &JobDescriptor) -> Self {
        let checkpoint_backend = job_descriptor
            .job_manager
            .job_properties
            .get_checkpoint()
            .map(|x| Some(x))
            .unwrap_or(None);

        let chain_cks = dashmap::DashMap::new();
        for (chain_id, operator_chain) in &job_graph.chain_map {
            let job_name = context.job_name.clone();
            let job_id = context.job_id.clone();
            let chain_id = *chain_id;
            let parallelism = operator_chain.parallelism;
            let storage = checkpoint_backend
                .as_ref()
                .map(|ck_backend| CheckpointStorageWrap::new(ck_backend));
            let chain_ck = ChainCheckpoint::new(job_name, job_id, chain_id, parallelism, storage);

            chain_cks.insert(chain_id, Arc::new(RwLock::new(chain_ck)));
        }

        CheckpointManager {
            job_name: context.job_name.clone(),
            chain_cks,
        }
    }

    pub fn add(&self, ck: Checkpoint) -> anyhow::Result<()> {
        match self.chain_cks.get_mut(&ck.chain_id) {
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
        let mut chain_checkpoints = HashMap::new();
        for entry in &self.chain_cks {
            let mut chain_ck = entry.value().write().unwrap();
            let checkpoints = chain_ck.load()?;
            chain_checkpoints.insert(*entry.key(), checkpoints);
        }

        Ok(chain_checkpoints)
    }

    pub fn get(&self) -> HashMap<ChainId, ChainCheckpoint> {
        let mut map = HashMap::new();
        for entry in &self.chain_cks {
            let chain_ck = entry.value().read().unwrap();
            map.insert(entry.key().clone(), chain_ck.clone());
        }

        map
    }
}

impl Clone for CheckpointManager {
    fn clone(&self) -> Self {
        let chain_cks = dashmap::DashMap::new();
        for entry in &self.chain_cks {
            chain_cks.insert(entry.key().clone(), entry.value().clone());
        }

        CheckpointManager {
            job_name: self.job_name.clone(),
            chain_cks,
        }
    }
}
