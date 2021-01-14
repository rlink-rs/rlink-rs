use std::collections::HashMap;

use crate::api::checkpoint::Checkpoint;
use crate::api::runtime::{CheckpointId, JobId, OperatorId};
use crate::storage::checkpoint::CheckpointStorage;

#[derive(Debug)]
pub struct MemoryCheckpointStorage {
    history_cks: HashMap<CheckpointId, Vec<Checkpoint>>,
}

impl MemoryCheckpointStorage {
    pub fn new() -> Self {
        MemoryCheckpointStorage {
            history_cks: HashMap::new(),
        }
    }
}

impl CheckpointStorage for MemoryCheckpointStorage {
    fn save(
        &mut self,
        _application_name: &str,
        _application_id: &str,
        checkpoint_id: CheckpointId,
        finish_cks: Vec<Checkpoint>,
        ttl: u64,
    ) -> anyhow::Result<()> {
        self.history_cks.insert(checkpoint_id, finish_cks);

        if checkpoint_id.0 < ttl {
            return Ok(());
        }

        let checkpoint_id_ttl = checkpoint_id.0 - ttl;
        let ttl_ck_ids: Vec<CheckpointId> = self
            .history_cks
            .iter()
            .map(|(ck_id, _cks)| *ck_id)
            .filter(|ck_id| ck_id.0 < checkpoint_id_ttl)
            .collect();

        for id in ttl_ck_ids {
            self.history_cks.remove(&id);
        }

        if self.history_cks.len() > 100 {
            let mut ttl_ck_ids: Vec<CheckpointId> = self
                .history_cks
                .iter()
                .map(|(ck_id, _cks)| *ck_id)
                .collect();
            ttl_ck_ids.sort_by_key(|x| x.0);
            for index in 0..self.history_cks.len() - 100 {
                let ck_id = ttl_ck_ids.get(index).unwrap();
                self.history_cks.remove(ck_id);
            }
        }

        Ok(())
    }

    fn load(
        &mut self,
        _application_name: &str,
        _job_id: JobId,
        _operator_id: OperatorId,
    ) -> anyhow::Result<Vec<Checkpoint>> {
        Ok(vec![])
    }
}
