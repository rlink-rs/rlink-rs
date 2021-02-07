use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::api::checkpoint::Checkpoint;
use crate::api::properties::SystemProperties;
use crate::api::runtime::{CheckpointId, JobId, OperatorId};
use crate::channel::{bounded, Receiver, Sender};
use crate::dag::metadata::DagMetadata;
use crate::runtime::context::Context;
use crate::runtime::ApplicationDescriptor;
use crate::storage::checkpoint::CheckpointStorage;
use std::time::Duration;

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
        application_descriptor: &ApplicationDescriptor,
    ) -> Self {
        let checkpoint_backend = application_descriptor
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
                        "checkpoint_id={:?} is not align, operators: {}",
                        checkpoint_id,
                        serde_json::to_string(&unreached_operators).unwrap()
                    );
                }
            }

            self.next_checkpoint(checkpoint_id);
        }

        match self.operator_cks.get_mut(&ck.operator_id) {
            Some(operator_checkpoint) => {
                info!("apply checkpoint: {:?}", ck);
                operator_checkpoint.apply(ck);
            }
            None => {
                return Err(anyhow!("operator not found, checkpoint={:?}", ck));
            }
        }

        if self.is_align() {
            let complete_checkpoint_id = self.current_ck_id;
            let complete_operator_cks = self.operator_cks.clone();
            // todo storage
            info!(
                "complete checkpoint_id={:?}, checkpoints: {:?}",
                complete_checkpoint_id, complete_operator_cks
            );

            self.finish_operator_cks = complete_operator_cks;
            self.next_checkpoint(checkpoint_id);
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
        let operator_checkpoints = HashMap::new();

        Ok(operator_checkpoints)
    }
}

impl Clone for CheckpointAlignManager {
    fn clone(&self) -> Self {
        CheckpointAlignManager {
            application_name: self.application_name.clone(),
            application_id: self.application_id.to_string(),
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
        application_descriptor: &ApplicationDescriptor,
    ) -> Self {
        let (sender, receiver) = bounded(100);
        CheckpointManager {
            ck_align_manager_task: Arc::new(RwLock::new(CheckpointAlignManager::new(
                dag_manager,
                context,
                application_descriptor,
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
