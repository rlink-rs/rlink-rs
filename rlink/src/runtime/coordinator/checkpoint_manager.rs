use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::api::checkpoint::Checkpoint;
use crate::api::properties::SystemProperties;
use crate::api::runtime::{CheckpointId, JobId, OperatorId};
use crate::dag::metadata::DagMetadata;
use crate::runtime::context::Context;
use crate::runtime::ApplicationDescriptor;
use crate::storage::checkpoint::{CheckpointStorage, TCheckpointStorage};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct OperatorCheckpoint {
    application_name: String,
    application_id: String,
    job_id: JobId,
    operator_id: OperatorId,
    operator_name: String,
    parallelism: u16,

    #[serde(skip_serializing, skip_deserializing)]
    storage: Option<CheckpointStorage>,

    current_ck_id: CheckpointId,
    /// Map<task_num, Checkpoint>
    current_cks: HashMap<u16, Checkpoint>,
    latest_finish_cks: Vec<Checkpoint>,
}

impl OperatorCheckpoint {
    pub fn new(
        application_name: String,
        application_id: String,
        job_id: JobId,
        operator_id: OperatorId,
        operator_name: String,
        parallelism: u16,
        storage: Option<CheckpointStorage>,
    ) -> Self {
        OperatorCheckpoint {
            application_name,
            application_id,
            job_id,
            operator_id,
            operator_name,
            parallelism,
            storage,
            current_ck_id: CheckpointId::default(),
            current_cks: HashMap::with_capacity(parallelism as usize),
            latest_finish_cks: Vec::with_capacity(parallelism as usize),
        }
    }

    pub fn apply(&mut self, ck: Checkpoint) -> anyhow::Result<()> {
        if ck.checkpoint_id.0 == self.current_ck_id.0 {
            if self.is_align() {
                Err(anyhow!("the Checkpoint has align. {:?}", &ck))
            } else if self.current_cks.contains_key(&ck.task_id.task_number) {
                Err(anyhow!("the Checkpoint has existed. {:?}", &ck))
            } else {
                self.current_ck_id = ck.checkpoint_id;
                self.current_cks.insert(ck.task_id.task_number, ck);

                self.archive_align();

                Ok(())
            }
        } else if ck.checkpoint_id.0 > self.current_ck_id.0 {
            if self.current_ck_id.0 != 0 && !self.is_align() {
                warn!("not all checkpoint is arrived");
            }

            self.current_ck_id = ck.checkpoint_id;

            self.current_cks.clear();
            self.current_cks.insert(ck.task_id.task_number, ck);

            self.archive_align();

            Ok(())
        } else {
            Err(anyhow!(
                "checkpoint_id={:?} late. current checkpoint_id={:?}, operator={}, job_id={}",
                ck.checkpoint_id,
                self.current_ck_id,
                self.operator_name,
                self.job_id.0
            ))
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
            Some(storage) => storage.load(
                self.application_name.as_str(),
                self.job_id,
                self.operator_id,
            ),
            None => Ok(vec![]),
        }
    }
}

impl Clone for OperatorCheckpoint {
    fn clone(&self) -> Self {
        OperatorCheckpoint {
            application_name: self.application_name.clone(),
            application_id: self.application_id.clone(),
            job_id: self.job_id,
            operator_id: self.operator_id,
            operator_name: self.operator_name.clone(),
            parallelism: self.parallelism,
            storage: None,
            current_ck_id: self.current_ck_id,
            current_cks: self.current_cks.clone(),
            latest_finish_cks: self.latest_finish_cks.clone(),
        }
    }
}

pub(crate) type JobCheckpointSafe = Arc<RwLock<OperatorCheckpoint>>;

#[derive(Debug)]
pub(crate) struct CheckpointManager {
    application_name: String,
    operator_cks: dashmap::DashMap<OperatorId, JobCheckpointSafe>,
}

impl CheckpointManager {
    pub fn new(
        dag_manager: &DagMetadata,
        context: &Context,
        application_descriptor: &ApplicationDescriptor,
    ) -> Self {
        let checkpoint_backend = application_descriptor
            .coordinator_manager
            .application_properties
            .get_checkpoint()
            .map(|x| Some(x))
            .unwrap_or(None);

        let operator_cks = dashmap::DashMap::new();
        for node in dag_manager.job_graph().nodes() {
            let application_name = context.application_name.clone();
            let application_id = context.application_id.clone();

            let job_node = node.detail();
            let parallelism = job_node.parallelism;
            let job_id = job_node.job_id;

            for stream_node in &job_node.stream_nodes {
                let operator_id = stream_node.id;
                let operator_name = stream_node.operator_name.clone();
                let storage = checkpoint_backend
                    .as_ref()
                    .map(|ck_backend| CheckpointStorage::new(ck_backend));

                let operator_ck = OperatorCheckpoint::new(
                    application_name.clone(),
                    application_id.clone(),
                    job_id,
                    operator_id,
                    operator_name,
                    parallelism,
                    storage,
                );

                operator_cks.insert(operator_id, Arc::new(RwLock::new(operator_ck)));
            }
        }

        CheckpointManager {
            application_name: context.application_name.clone(),
            operator_cks,
        }
    }

    pub fn apply(&self, ck: Checkpoint) -> anyhow::Result<()> {
        match self.operator_cks.get_mut(&ck.operator_id) {
            Some(mut d) => {
                let mut operator_checkpoint = d.value_mut().write().unwrap();
                operator_checkpoint.apply(ck)
            }
            None => Err(anyhow!("checkpoint_id={:?} not found", ck)),
        }
    }

    pub fn load(&mut self) -> anyhow::Result<HashMap<OperatorId, Vec<Checkpoint>>> {
        let mut operator_checkpoints = HashMap::new();
        for entry in &self.operator_cks {
            let mut operator_ck = entry.value().write().unwrap();
            let checkpoints = operator_ck.load()?;
            operator_checkpoints.insert(*entry.key(), checkpoints);
        }

        Ok(operator_checkpoints)
    }

    pub fn get(&self) -> HashMap<OperatorId, OperatorCheckpoint> {
        let mut map = HashMap::new();
        for entry in &self.operator_cks {
            let operator_ck = entry.value().read().unwrap();
            map.insert(entry.key().clone(), operator_ck.clone());
        }

        map
    }
}

impl Clone for CheckpointManager {
    fn clone(&self) -> Self {
        let operator_cks = dashmap::DashMap::new();
        for entry in &self.operator_cks {
            operator_cks.insert(entry.key().clone(), entry.value().clone());
        }

        CheckpointManager {
            application_name: self.application_name.clone(),
            operator_cks,
        }
    }
}
