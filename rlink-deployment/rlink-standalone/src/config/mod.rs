use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use rlink::api::cluster::load_config;

use crate::utils::get_work_space;

pub type ClusterConfig = rlink::api::cluster::ClusterConfig;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Context {
    pub config: ClusterConfig,
    pub config_path: PathBuf,
    pub script_path: PathBuf,
    pub task_managers: Vec<String>,
}

pub fn create_context() -> anyhow::Result<Context> {
    let work_space = get_work_space();
    let conf_path = work_space.clone().join("config");

    let standalone_file = conf_path.clone().join("standalone.yaml");
    let config = load_config(standalone_file.clone())?;

    let script_path = conf_path.clone().join("run_task.sh");

    let task_managers_file = conf_path.clone().join("task_managers");
    let task_managers = load_task_managers(task_managers_file);

    Ok(Context {
        config,
        config_path: standalone_file,
        script_path,
        task_managers,
    })
}

pub fn load_task_managers(path: PathBuf) -> Vec<String> {
    let file = File::open(path).unwrap();
    let fin = BufReader::new(file);

    let mut lines = Vec::new();
    for line in fin.lines() {
        let line = line.unwrap();
        if !line.is_empty() {
            lines.push(line);
        }
    }

    lines
}
