use crate::api::backend::{OperatorState, StateValue};
use crate::runtime::CheckpointId;
use crate::storage::operator_state::dfs_state_manager::DFSListStateManager;
use crate::storage::operator_state::{OperatorStateManager, StateName};
use std::collections::HashMap;

impl StateName {
    pub fn parse(file_name: &str) -> Result<Self, std::io::Error> {
        let (file_name, progress) = if file_name.ends_with(".progress") {
            (&file_name[0..file_name.len() - 9], true)
        } else {
            (file_name, false)
        };

        let pairs: Vec<&str> = file_name.split(".").collect();
        if pairs.len() != 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid file name",
            ));
        }

        let job_id = StateName::parse_string(pairs[0], "job")?;
        let checkpoint_id = StateName::parse_num::<u64>(pairs[1], "ckpt")?;
        let chain_id = StateName::parse_num::<u32>(pairs[2], "cid")?;
        let task_number = StateName::parse_num::<u16>(pairs[3], "tn")?;

        Ok(StateName {
            job_id,
            checkpoint_id,
            chain_id,
            task_number,
            progress,
        })
    }

    fn parse_string(block: &str, field: &str) -> Result<String, std::io::Error> {
        let job_id_pair: Vec<&str> = block.split("_").collect();
        if job_id_pair.len() != 2 && job_id_pair[0].ne(field) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid file name, `job` not found",
            ));
        }
        Ok(job_id_pair[1].to_string())
    }

    fn parse_num<T: std::str::FromStr>(block: &str, field: &str) -> Result<T, std::io::Error> {
        let checkpoint_id_pair: Vec<&str> = block.split("_").collect();
        if checkpoint_id_pair.len() != 2 && checkpoint_id_pair[0].ne(field) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid file name, `ckpt` not found",
            ));
        }
        match T::from_str(checkpoint_id_pair[1]) {
            Ok(t) => Ok(t),
            Err(_e) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "parse error",
            )),
        }
    }

    pub fn to_file_name(&self) -> String {
        format!(
            "job_{}.ckpt_{}.cid_{}.tn_{}",
            self.job_id, self.checkpoint_id, self.chain_id, self.task_number
        )
    }
}

#[derive(Debug)]
pub struct DFSListState {
    pub(crate) job_id: String,
    pub(crate) task_number: u16,

    pub(crate) manager: DFSListStateManager,
    pub(crate) checkpoint_id: CheckpointId,
    pub(crate) values: Vec<String>,
}

impl DFSListState {
    pub(crate) fn new(job_id: String, task_number: u16, manager: DFSListStateManager) -> Self {
        DFSListState {
            job_id,
            task_number,
            manager,
            checkpoint_id: 0,
            values: Vec::new(),
        }
    }
}

impl OperatorState for DFSListState {
    fn update(&mut self, checkpoint_id: u64, values: Vec<String>) {
        self.checkpoint_id = checkpoint_id;
        self.values.clear();
        self.values.extend_from_slice(values.as_slice());
    }

    fn snapshot(&mut self) -> std::io::Result<()> {
        let ck_file = StateName::new(
            self.job_id.clone(),
            self.manager.chain_id,
            self.task_number,
            self.checkpoint_id,
        );
        self.manager.save(&ck_file, &self.values)
    }

    fn load_latest(
        &self,
        checkpoint_id: CheckpointId,
    ) -> std::io::Result<HashMap<u16, StateValue>> {
        let reader = self.manager.as_reader()?;
        let checkpoint_states = reader.get_checkpoint_states().get(&checkpoint_id);
        match checkpoint_states {
            Some(state_names) => {
                let mut state_values = HashMap::new();
                for state_name in state_names {
                    let values = self.manager.load_checkpoint_value(state_name)?;
                    state_values.insert(state_name.task_number, StateValue::new(values));
                }

                Ok(state_values)
            }
            None => Ok(HashMap::new()),
        }
    }
}
