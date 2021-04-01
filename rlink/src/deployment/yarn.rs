use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::process::Stdio;
use std::sync::Arc;

use crate::api::cluster::TaskResourceInfo;
use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::channel::{bounded, Receiver, Sender};
use crate::deployment::TResourceManager;
use crate::runtime::context::Context;
use crate::runtime::{ClusterDescriptor, ManagerType};
use crate::utils;

#[derive(Debug)]
pub(crate) struct YarnResourceManager {
    context: Arc<Context>,
    cluster_descriptor: Option<ClusterDescriptor>,

    yarn_command: Option<YarnCliCommand>,
}

impl YarnResourceManager {
    pub fn new(context: Arc<Context>) -> Self {
        YarnResourceManager {
            context,
            cluster_descriptor: None,
            yarn_command: None,
        }
    }
}

impl TResourceManager for YarnResourceManager {
    fn prepare(&mut self, context: &Context, job_descriptor: &ClusterDescriptor) {
        self.cluster_descriptor = Some(job_descriptor.clone());

        self.yarn_command = Some(YarnCliCommand::new(&context, job_descriptor));
    }

    fn worker_allocate<S>(
        &self,
        _stream_app: &S,
        _stream_env: &StreamExecutionEnvironment,
    ) -> anyhow::Result<Vec<TaskResourceInfo>>
    where
        S: StreamApp + 'static,
    {
        let cluster_descriptor = self.cluster_descriptor.as_ref().unwrap();

        let mut task_args = Vec::new();
        for task_manager_descriptor in &cluster_descriptor.worker_managers {
            let mut args = HashMap::new();
            args.insert(
                "cluster_mode".to_string(),
                self.context.cluster_mode.to_string(),
            );
            args.insert("manager_type".to_string(), ManagerType::Worker.to_string());
            args.insert(
                "application_id".to_string(),
                self.context.application_id.clone(),
            );
            args.insert(
                "task_manager_id".to_string(),
                task_manager_descriptor.task_manager_id.clone(),
            );
            args.insert(
                "coordinator_address".to_string(),
                cluster_descriptor
                    .coordinator_manager
                    .coordinator_address
                    .clone(),
            );

            task_args.push(args);
        }

        self.yarn_command.as_ref().unwrap().allocate(task_args)
    }

    fn stop_workers(&self, task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        self.yarn_command.as_ref().unwrap().stop(task_ids)
    }
}

#[derive(Serialize, Deserialize)]
struct Data<T> {
    cmd: String,
    cmd_id: String,
    data: T,
}

impl<T> Data<T> {
    fn new(cmd: String, cmd_id: String, data: T) -> Self {
        Data { cmd, cmd_id, data }
    }
}

type Command = Data<Vec<HashMap<String, String>>>;
type Response = Data<Vec<TaskResourceInfo>>;

type StopCommand = Data<Vec<TaskResourceInfo>>;

const COMMAND_PREFIX: &'static str = "/*rlink-rs_yarn*/";

fn parse_command(command_line: &str) -> Option<&str> {
    command_line
        .find(COMMAND_PREFIX)
        .map(|pos| &command_line[pos + COMMAND_PREFIX.len()..command_line.len()])
}

fn parse_line(command_line: &std::io::Result<String>) -> Option<&str> {
    command_line
        .as_ref()
        .map(|x| parse_command(x.as_str()))
        .unwrap_or(None)
}

#[derive(Debug)]
struct YarnCliCommand {
    cmd_sender: Sender<String>,
    ret_receiver: Receiver<String>,
}

impl YarnCliCommand {
    pub fn new(context: &Context, cluster_descriptor: &ClusterDescriptor) -> Self {
        let coordinator_manager = &cluster_descriptor.coordinator_manager;

        let child = std::process::Command::new("java")
            .arg("-Xmx256M")
            // .arg("rlink.yarn.manager.ResourceManagerCli")
            .arg(context.yarn_manager_main_class.as_str())
            .arg("--coordinator_address")
            .arg(coordinator_manager.coordinator_address.as_str())
            .arg("--worker_process_path")
            .arg(context.worker_process_path.as_str())
            .arg("--memory_mb")
            .arg(coordinator_manager.memory_mb.to_string())
            .arg("--v_cores")
            .arg(coordinator_manager.v_cores.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        let (cmd_sender, cmd_receiver) = bounded::<String>(2);
        let (ret_sender, ret_receiver) = bounded::<String>(200);

        match child.stdin {
            Some(mut stdin) => {
                std::thread::spawn(move || {
                    let cmd_receiver = cmd_receiver;
                    loop {
                        match cmd_receiver.recv() {
                            Ok(cmd) => {
                                let c = format!("{}\n", cmd);
                                info!("cmd_receiver cmd={}", c);
                                match stdin.write(c.as_bytes()) {
                                    Ok(_) => {}
                                    Err(e) => error!("stdin write error. {}", e),
                                }
                            }
                            Err(e) => {
                                panic!("stdin recv error. {}", e);
                            }
                        }
                    }
                });
            }
            None => error!("stdin not found"),
        };
        match child.stderr {
            Some(stderr) => {
                let ret_sender = ret_sender.clone();
                std::thread::spawn(move || {
                    std::io::BufReader::new(stderr).lines().for_each(|txt| {
                        error!("command line: {:?}", txt);
                        parse_line(&txt)
                            .map(|command| ret_sender.send(command.to_string()).unwrap());
                    });
                });
            }
            _ => {}
        };
        match child.stdout {
            Some(stdout) => {
                std::thread::spawn(move || {
                    std::io::BufReader::new(stdout).lines().for_each(|txt| {
                        info!("command line: {:?}", txt);
                        parse_line(&txt)
                            .map(|command| ret_sender.send(command.to_string()).unwrap());
                    });
                });
            }
            _ => {}
        };

        YarnCliCommand {
            cmd_sender,
            ret_receiver,
        }
    }

    /// cmd: CommandName CommandId data(`json`)
    /// ret: /*Rust*/ CommandId data(`json`)
    pub fn allocate(
        &self,
        task_args: Vec<HashMap<String, String>>,
    ) -> anyhow::Result<Vec<TaskResourceInfo>> {
        let cmd_id = utils::generator::gen_with_ts();
        let command = Command::new("allocate".to_string(), cmd_id.to_string(), task_args);
        let command_json = serde_json::to_string(&command).unwrap();

        let cmd_str = format!("{}\n", command_json);

        info!("send command: {}", cmd_str);
        match self.cmd_sender.send(cmd_str) {
            Ok(_) => {}
            Err(e) => {
                error!("send command error. {}", e);
            }
        }

        let txt = self.ret_receiver.recv()?;
        let response = serde_json::from_slice::<Response>(txt.as_bytes())?;

        if !response.cmd_id.eq(cmd_id.as_str()) {
            Err(anyhow::Error::msg("`cmd_id` is inconsistency"))
        } else {
            Ok(response.data)
        }
    }

    pub fn stop(&self, task_infos: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        let cmd_id = utils::generator::gen_with_ts();
        let command = StopCommand::new("stop".to_string(), cmd_id.to_string(), task_infos);
        let command_json = serde_json::to_string(&command).unwrap();

        let cmd_str = format!("{}\n", command_json);

        info!("send command: {}", cmd_str);
        match self.cmd_sender.send(cmd_str) {
            Ok(_) => {}
            Err(e) => {
                error!("send command error. {}", e);
            }
        }

        let txt = self.ret_receiver.recv()?;
        let response = serde_json::from_slice::<Response>(txt.as_bytes())?;

        if !response.cmd_id.eq(cmd_id.as_str()) {
            Err(anyhow::Error::msg("`cmd_id` is inconsistency"))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::deployment::yarn::Data;
    use crate::utils;

    #[test]
    pub fn command_json_test() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), "b".to_string());

        let mut task_args: Vec<HashMap<String, String>> = Vec::new();
        task_args.push(map);

        let cmd_id = utils::generator::gen_with_ts();
        let command = Data::new("allocate".to_string(), cmd_id, task_args);
        let command_json = serde_json::to_string(&command).unwrap();

        let cmd_str = format!("{}\n", command_json);

        println!("{}", cmd_str)
    }
}
