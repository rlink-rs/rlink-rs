use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::channel::{bounded, Receiver, Sender};
use crate::core::checkpoint::Checkpoint;
use crate::core::properties::SystemProperties;
use crate::core::runtime::{CheckpointId, JobId, OperatorId};
use crate::dag::metadata::DagMetadata;
use crate::runtime::context::Context;
use crate::runtime::ClusterDescriptor;
use crate::storage::checkpoint::{CheckpointStorage, TCheckpointStorage};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct OperatorCheckpoint {
    job_id: JobId,
    operator_id: OperatorId,
    operator_name: String,
    parallelism: u16,

    /// Map<task_num, Checkpoint>
    current_cks: HashMap<u16, Checkpoint>,
}

impl OperatorCheckpoint {
    pub fn new(
        job_id: JobId,
        operator_id: OperatorId,
        operator_name: String,
        parallelism: u16,
    ) -> Self {
        OperatorCheckpoint {
            job_id,
            operator_id,
            operator_name,
            parallelism,
            current_cks: HashMap::with_capacity(parallelism as usize),
        }
    }

    pub fn apply(&mut self, ck: Checkpoint) {
        if self.is_align() {
            warn!("the Checkpoint has align. {:?}", &ck);
            return;
        }

        if self.current_cks.contains_key(&ck.task_id.task_number) {
            warn!("the Checkpoint has existed. {:?}", &ck);
            return;
        }

        self.current_cks.insert(ck.task_id.task_number, ck);
    }

    fn is_align(&self) -> bool {
        self.current_cks.len() == self.parallelism as usize
    }
}

impl Clone for OperatorCheckpoint {
    fn clone(&self) -> Self {
        OperatorCheckpoint {
            job_id: self.job_id,
            operator_id: self.operator_id,
            operator_name: self.operator_name.clone(),
            parallelism: self.parallelism,
            current_cks: self.current_cks.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CheckpointAlignManager {
    application_name: String,
    application_id: String,
    checkpoint_ttl: Duration,

    current_ck_id: CheckpointId,
    operator_cks: HashMap<OperatorId, OperatorCheckpoint>,
    finish_operator_cks: HashMap<OperatorId, OperatorCheckpoint>,

    #[serde(skip_serializing, skip_deserializing)]
    storage: Option<CheckpointStorage>,
}

impl CheckpointAlignManager {
    pub fn new(
        dag_manager: &DagMetadata,
        context: &Context,
        cluster_descriptor: &ClusterDescriptor,
        checkpoint_ttl: Duration,
    ) -> Self {
        let checkpoint_backend = cluster_descriptor
            .coordinator_manager
            .application_properties
            .get_checkpoint()
            .map(|x| Some(x))
            .unwrap_or(None);
        let storage = checkpoint_backend
            .as_ref()
            .map(|ck_backend| CheckpointStorage::new(ck_backend));

        let mut operator_cks = HashMap::new();
        for node in dag_manager.job_graph().nodes() {
            let job_node = node.detail();
            let parallelism = job_node.parallelism;
            let job_id = job_node.job_id;

            for stream_node in &job_node.stream_nodes {
                let operator_id = stream_node.id;
                let operator_name = stream_node.operator_name.clone();

                let operator_ck =
                    OperatorCheckpoint::new(job_id, operator_id, operator_name, parallelism);

                operator_cks.insert(operator_id, operator_ck);
            }
        }

        CheckpointAlignManager {
            application_name: context.application_name.clone(),
            application_id: context.application_id.clone(),
            checkpoint_ttl,
            current_ck_id: CheckpointId::default(),
            operator_cks,
            finish_operator_cks: HashMap::new(),
            storage,
        }
    }

    pub fn apply(&mut self, ck: Checkpoint) -> anyhow::Result<()> {
        let checkpoint_id = ck.checkpoint_id;
        if self.current_ck_id.0 > checkpoint_id.0 {
            warn!(
                "checkpoint_id={:?} late. current checkpoint_id={:?}, operator={:?}, task_id={:?}",
                ck.checkpoint_id, self.current_ck_id, ck.operator_id, ck.task_id,
            );

            return Ok(());
        } else if self.current_ck_id.0 < checkpoint_id.0 {
            if !self.current_ck_id.is_default() {
                let unreached_operators = self.unreached_operators();
                if unreached_operators.len() > 0 {
                    warn!(
                        "the new checkpoint reached, found un-align checkpoint_id={:?}, un-align operators: {}",
                        self.current_ck_id,
                        serde_json::to_string(&unreached_operators).unwrap()
                    );
                }
            }

            self.next_checkpoint(checkpoint_id);
        }

        match self.operator_cks.get_mut(&ck.operator_id) {
            Some(operator_checkpoint) => {
                operator_checkpoint.apply(ck);
            }
            None => {
                return Err(anyhow!("operator not found, checkpoint={:?}", ck));
            }
        }

        if self.is_align() {
            let complete_checkpoint_id = self.current_ck_id;
            let complete_operator_cks = self.operator_cks.clone();

            debug!(
                "complete checkpoint_id={:?}, checkpoints: {:?}",
                complete_checkpoint_id, complete_operator_cks
            );
            self.finish_operator_cks = complete_operator_cks;

            match self.storage.as_mut() {
                Some(storage) => {
                    let cks = {
                        let mut cks = Vec::new();
                        self.finish_operator_cks.iter().for_each(|(_, v)| {
                            let operator_cks: Vec<Checkpoint> =
                                v.current_cks.iter().map(|x| x.1.clone()).collect();
                            cks.extend_from_slice(operator_cks.as_slice());
                        });
                        cks
                    };

                    storage.save(
                        self.application_name.as_str(),
                        self.application_id.as_str(),
                        complete_checkpoint_id,
                        cks,
                        self.checkpoint_ttl.as_millis() as u64,
                    )?;
                }
                None => {}
            }
        }

        Ok(())
    }

    fn unreached_operators(&self) -> Vec<&OperatorCheckpoint> {
        let align_operators: Vec<&OperatorCheckpoint> = self
            .operator_cks
            .iter()
            .filter(|(_, operator_checkpoint)| !operator_checkpoint.is_align())
            .map(|(_, operator_checkpoint)| operator_checkpoint)
            .collect();
        align_operators
    }

    #[inline]
    fn is_align(&self) -> bool {
        self.unreached_operators().len() == 0
    }

    fn next_checkpoint(&mut self, checkpoint_id: CheckpointId) {
        self.current_ck_id = checkpoint_id;
        self.operator_cks = {
            let mut operator_cks = HashMap::new();
            for (operator_id, operator_checkpoint) in &self.operator_cks {
                operator_cks.insert(
                    operator_id.clone(),
                    OperatorCheckpoint::new(
                        operator_checkpoint.job_id.clone(),
                        operator_checkpoint.operator_id.clone(),
                        operator_checkpoint.operator_name.clone(),
                        operator_checkpoint.parallelism,
                    ),
                );
            }
            operator_cks
        }
    }

    pub fn load(&mut self) -> anyhow::Result<HashMap<OperatorId, Vec<Checkpoint>>> {
        let mut operator_checkpoints = HashMap::new();

        if let Some(storage) = self.storage.as_mut() {
            let mut checkpoints = storage.load_v2(self.application_name.as_str())?;
            let completed_checkpoint_id = checkpoints
                .iter()
                .filter(|c|c.completed_checkpoint_id>0)
                .min_by_key(|c| c.completed_checkpoint_id.unwrap_or_default())
                .map(|c| c.completed_checkpoint_id.unwrap_or_default())
                .unwrap_or_default();

            if !completed_checkpoint_id.is_default() {
                checkpoints = storage.load_by_checkpoint_id(
                    self.application_name.as_str(),
                    completed_checkpoint_id,
                )?;
            }

            for checkpoint in checkpoints {
                operator_checkpoints
                    .entry(checkpoint.operator_id)
                    .or_insert(Vec::new())
                    .push(checkpoint);
            }
        }

        Ok(operator_checkpoints)
    }
}

impl Clone for CheckpointAlignManager {
    fn clone(&self) -> Self {
        CheckpointAlignManager {
            application_name: self.application_name.clone(),
            application_id: self.application_id.to_string(),
            checkpoint_ttl: self.checkpoint_ttl,
            current_ck_id: CheckpointId::default(),
            operator_cks: self.operator_cks.clone(),
            finish_operator_cks: self.finish_operator_cks.clone(),
            storage: None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct CheckpointManager {
    ck_align_manager_task: Arc<RwLock<CheckpointAlignManager>>,

    sender: Sender<Checkpoint>,
    receiver: Receiver<Checkpoint>,
}

impl CheckpointManager {
    pub fn new(
        dag_manager: &DagMetadata,
        context: &Context,
        cluster_descriptor: &ClusterDescriptor,
        checkpoint_ttl: Duration,
    ) -> Self {
        let (sender, receiver) = bounded(100);
        CheckpointManager {
            ck_align_manager_task: Arc::new(RwLock::new(CheckpointAlignManager::new(
                dag_manager,
                context,
                cluster_descriptor,
                checkpoint_ttl,
            ))),
            sender,
            receiver,
        }
    }

    pub fn run_align_task(&self) {
        let task = self.ck_align_manager_task.clone();
        let receiver = self.receiver.clone();
        crate::utils::thread::spawn("ck_align_mgr", move || {
            while let Ok(checkpoint) = receiver.recv() {
                let mut ck_align_manager = task.write().unwrap();
                match ck_align_manager.apply(checkpoint) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("apply checkpoint error. {}", e);
                    }
                }
            }

            error!("checkpoint manager task finish");
        });
    }

    pub fn apply(&self, ck: Checkpoint) -> anyhow::Result<()> {
        self.sender.send_timeout(ck, Duration::from_secs(2))?;
        Ok(())
    }

    pub fn get(&self) -> CheckpointAlignManager {
        let ck_align_manager = self.ck_align_manager_task.read().unwrap();
        ck_align_manager.clone()
    }

    pub fn load(&mut self) -> anyhow::Result<HashMap<OperatorId, Vec<Checkpoint>>> {
        let mut ck_align_manager = self.ck_align_manager_task.write().unwrap();
        ck_align_manager.load()
    }
}
