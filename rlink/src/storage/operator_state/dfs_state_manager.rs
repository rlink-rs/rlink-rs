use crate::runtime::{ChainId, CheckpointId};
use crate::storage::operator_state::dfs_state::DFSListState;
use crate::storage::operator_state::{
    OperatorState, OperatorStateManager, OperatorStateReader, StateName,
};
use crate::utils::fs::{read_file, write_file};
use std::collections::HashMap;
use std::fs::DirBuilder;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct DFSListStateManager {
    pub(crate) chain_id: ChainId,
    pub(crate) path: PathBuf,
}

impl DFSListStateManager {
    pub fn new(chain_id: ChainId, path: PathBuf) -> Self {
        let path = path.join(format!("chain_{}", chain_id));
        DFSListStateManager { chain_id, path }
    }
}

impl OperatorStateManager for DFSListStateManager {
    fn prepare(&mut self, parallelism: u32) -> std::io::Result<()> {
        info!(
            "Recursive make dir {} for state chain_{}",
            self.path.to_str().unwrap(),
            self.chain_id
        );
        for task_number in 0..parallelism {
            let path = self.path.join(format!("{}", task_number));
            DirBuilder::new().recursive(true).create(path)?;
        }

        Ok(())
    }

    fn save(&mut self, state_name: &StateName, values: &Vec<String>) -> std::io::Result<()> {
        let ck_file = state_name.to_file_name();
        let ck_file_progress = format!("{}.progress", ck_file);
        let path = self.path.join(format!("{}", state_name.task_number));
        write_file(path.clone(), ck_file_progress.as_str(), values)?;

        std::fs::rename(path.join(ck_file_progress), path.join(ck_file))
    }

    fn load(&self) -> std::io::Result<Vec<StateName>> {
        let chain_dirs = std::fs::read_dir(self.path.clone())?;
        let mut checkpoint_files = Vec::new();

        // walk chain dir
        for chain_dir in chain_dirs {
            if let Ok(chain_dir_entry) = chain_dir {
                if !chain_dir_entry.metadata().unwrap().is_dir() {
                    continue;
                }

                // walk task dir
                let task_dirs = std::fs::read_dir(chain_dir_entry.path())?;
                for task_dir in task_dirs {
                    if let Ok(task_dir_entry) = task_dir {
                        // parse file name to `StateName`
                        let file_name = task_dir_entry.file_name();
                        let file_name = file_name.to_str().unwrap();
                        match StateName::parse(file_name) {
                            Ok(file_name) => checkpoint_files.push(file_name),
                            Err(e) => error!("found illegal file({}). {}", file_name, e),
                        }
                    }
                }
            }
        }

        Ok(checkpoint_files)
    }

    fn load_as_map(&self) -> std::io::Result<HashMap<CheckpointId, Vec<StateName>>> {
        let state_names = self.load()?;
        let mut checkpoint_map = HashMap::new();
        for state_name in state_names {
            let tasks = checkpoint_map
                .entry(state_name.checkpoint_id)
                .or_insert(Vec::new());
            tasks.push(state_name);
        }

        Ok(checkpoint_map)
    }

    fn delete(&mut self, state_name: &StateName) -> std::io::Result<()> {
        let path = self
            .path
            .join(format!("{}", state_name.task_number))
            .join(state_name.to_file_name());
        std::fs::remove_file(path)
    }

    fn as_reader(&self) -> std::io::Result<OperatorStateReader> {
        let c = self.load_as_map()?;
        Ok(OperatorStateReader::new(c))
    }

    fn create_state(&self, job_id: String, task_number: u16) -> Box<dyn OperatorState> {
        let state = DFSListState::new(job_id, task_number, self.clone());
        let state: Box<dyn OperatorState> = Box::new(state);
        state
    }

    fn load_checkpoint_value(&self, state_name: &StateName) -> std::io::Result<Vec<String>> {
        let ck_file = state_name.to_file_name();
        let path = self
            .path
            .join(format!("{}", state_name.task_number))
            .join(ck_file);
        let content = read_file(path)?;
        let values: Vec<&str> = content.split("\n").collect();
        let values: Vec<String> = values.iter().map(|x| x.to_string()).collect();
        Ok(values)
    }
}
