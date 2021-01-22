use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::process::Stdio;

use serde::Deserialize;
use serde::Serialize;

use crate::api::cluster::TaskResourceInfo;
use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::channel::{bounded, Receiver, Sender};
use crate::deployment::TResourceManager;
use crate::runtime::context::Context;
use crate::runtime::{ApplicationDescriptor, ManagerType};
use crate::utils;

#[derive(Debug)]
pub(crate) struct YarnResourceManager {
    context: Context,
    job_descriptor: Option<ApplicationDescriptor>,

    yarn_command: Option<YarnCliCommand>,
}

impl YarnResourceManager {
    pub fn new(context: Context) -> Self {
        YarnResourceManager {
            context,
            job_descriptor: None,
            yarn_command: None,
        }
    }
}

impl TResourceManager for YarnResourceManager {
    fn prepare(&mut self, context: &Context, job_descriptor: &ApplicationDescriptor) {
        self.job_descriptor = Some(job_descriptor.clone());

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
        let application_descriptor = self.job_descriptor.as_ref().unwrap();

        let mut task_args = Vec::new();
        for task_manager_descriptor in &application_descriptor.worker_managers {
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
                application_descriptor
                    .coordinator_manager
                    .coordinator_address
                    .clone(),
            );

            task_args.push(args);
        }

        self.yarn_command.as_ref().unwrap().allocate(task_args)
    }

    fn stop_workers(&self, _task_ids: Vec<TaskResourceInfo>) -> anyhow::Result<()> {
        unimplemented!()
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

#[derive(Debug)]
struct YarnCliCommand {
    cmd_sender: Sender<String>,
    ret_receiver: Receiver<String>,
}

impl YarnCliCommand {
    pub fn new(context: &Context, job_descriptor: &ApplicationDescriptor) -> Self {
        let child = std::process::Command::new("java")
            .arg("-Xmx256M")
            .arg("rlink.yarn.manager.ResourceManagerCli")
            .arg("--coordinator_address")
            .arg(
                job_descriptor
                    .coordinator_manager
                    .coordinator_address
                    .as_str(),
            )
            .arg("--worker_process_path")
            .arg(context.worker_process_path.as_str())
            .arg("--memory_mb")
            .arg(context.memory_mb.to_string())
            .arg("--v_cores")
            .arg(context.v_cores.to_string())
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
                            Err(e) => error!("stdin recv error. {}", e),
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
                        let txt = txt.unwrap();
                        error!("{}", txt);

                        match txt.find("/*Rust*/") {
                            Some(pos) => {
                                let ret = &txt[pos + 8..txt.len()];
                                ret_sender.send(ret.to_string()).unwrap();
                            }
                            None => {}
                        }
                    });
                });
            }
            _ => {}
        };
        match child.stdout {
            Some(stdout) => {
                std::thread::spawn(move || {
                    std::io::BufReader::new(stdout).lines().for_each(|txt| {
                        let txt = txt.unwrap();
                        info!("{}", txt);

                        match txt.find("/*Rust*/") {
                            Some(pos) => {
                                let ret = &txt[pos + 8..txt.len()];
                                ret_sender.send(ret.to_string()).unwrap();
                            }
                            None => {}
                        }
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
