use std::convert::TryFrom;
use std::path::PathBuf;
use std::str::FromStr;

use crate::api::cluster::{load_config, ClusterConfig};
use crate::metrics::global_metrics::set_manager_id;
use crate::runtime::{logger, ClusterMode, ManagerType};
use crate::utils;
use crate::utils::process::{parse_arg, work_space};

/// Process run context
/// `cluster_mode`: Empty or `Standalone`, default `Local`, generated by `StandaloneResourceManager`
/// `manager_type`: `Coordinator` or `Worker`, generated by `StandaloneResourceManager`
///
/// `Local` and `Coordinator` process args:
///     `bind_ip`: ignore, default with "0.0.0.0"
///     `task_manager_id`: ignore
///     `num_task_managers`: ignore task manager size
///     `cluster_config`: ignore
/// `Local` and `Worker` process args:
///     `bind_ip`: ignore, default with "0.0.0.0"
///     `task_manager_id`: task manager process id, generated by `Coordinator`
///     `num_task_managers`: ignore
///     `cluster_config`: ignore
///
/// `Standalone` mode
///     `Coordinator` process args:
///         `cluster_mode`: must be `Standalone`
///         `manager_type`: must be `Coordinator`
///         `num_task_managers`: task manager size
///         `coordinator_address`: ignore
///         `bind_ip`: coordinator ip, generated by `TaskManager`
///         `job_id`: job id, generated by `JobManager`
///         `task_manager_id`: ignore
///         `cluster_config`: cluster config path, generated by `TaskManager`
///     `Worker` process args:
///         `cluster_mode`: must be `Standalone`
///         `manager_type`: must be `Worker`
///         `num_task_managers`: ignore
///         `coordinator_address`: coordinator address
///         `bind_ip`: worker ip, generated by `TaskManager`
///         `job_id`: job id, same as `Coordinator`
///         `task_manager_id`: task manager process id, generated by `Coordinator`
///         `cluster_config`: cluster config path, generated by `TaskManager`
///
#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct Context {
    pub application_name: String,
    pub application_id: String,
    /// when `ManagerType::Coordinator`: `job_manager_id`
    /// when `ManagerType::Worker`: `task_manager_id`
    pub task_manager_id: String,
    pub bind_ip: String,
    pub cluster_mode: ClusterMode,
    pub num_task_managers: u32,
    pub manager_type: ManagerType,
    pub cluster_config: ClusterConfig,
    pub metric_addr: String,
    /// effective only in `Worker` mode
    pub coordinator_address: String,
    pub dashboard_path: String,

    /// on yarn arg
    pub yarn_manager_main_class: String,
    pub worker_process_path: String,
    pub memory_mb: usize,
    pub v_cores: usize,
}

impl Context {
    pub fn new(
        application_name: String,
        application_id: String,
        task_manager_id: String,
        bind_ip: String,
        cluster_mode: ClusterMode,
        num_task_managers: u32,
        manager_type: ManagerType,
        cluster_config: ClusterConfig,
        metric_addr: String,
        coordinator_address: String,
        dashboard_path: String,
        yarn_manager_main_class: String,
        worker_process_path: String,
        memory_mb: usize,
        v_cores: usize,
    ) -> Self {
        Context {
            application_name,
            application_id,
            task_manager_id,
            bind_ip,
            cluster_mode,
            num_task_managers,
            manager_type,
            cluster_config,
            metric_addr,
            coordinator_address,
            dashboard_path,
            yarn_manager_main_class,
            worker_process_path,
            memory_mb,
            v_cores,
        }
    }

    pub fn parse_node_arg(application_name: &str) -> anyhow::Result<Context> {
        let bind_ip = utils::ip::get_service_ip()?.to_string();

        let cluster_mode = match parse_arg("cluster_mode") {
            Ok(value) => ClusterMode::try_from(value.as_str())?,
            Err(_e) => ClusterMode::Local,
        };

        let manager_type = match parse_arg("manager_type") {
            Ok(manager_type) => ManagerType::try_from(manager_type.as_str())?,
            Err(_e) => ManagerType::Coordinator,
        };

        let application_id = match cluster_mode {
            ClusterMode::Local => utils::generator::gen_with_ts(),
            ClusterMode::Standalone | ClusterMode::YARN | ClusterMode::Kubernetes => {
                parse_arg("application_id")?
            }
        };

        let task_manager_id = match manager_type {
            ManagerType::Coordinator => "coordinator".to_string(),
            ManagerType::Worker => parse_arg("task_manager_id")?,
        };
        set_manager_id(task_manager_id.as_str(), bind_ip.as_str());

        let num_task_managers = match manager_type {
            ManagerType::Coordinator => match cluster_mode {
                ClusterMode::Local => 1,
                ClusterMode::Standalone | ClusterMode::YARN | ClusterMode::Kubernetes => {
                    let num_task_managers = parse_arg("num_task_managers")?;
                    let num_task_managers =
                        u32::from_str(num_task_managers.as_str()).map_err(|_e| {
                            anyhow!(
                                "parse `num_task_managers`=`{}` to u32 error",
                                num_task_managers
                            )
                        })?;
                    if num_task_managers < 1 {
                        return Err(anyhow!("`num_task_managers` must the [value > 1]"));
                    }
                    num_task_managers
                }
            },
            _ => 0,
        };

        let cluster_config = match cluster_mode {
            ClusterMode::Local => match parse_arg("cluster_config") {
                Ok(cluster_config) => load_config(PathBuf::from(cluster_config))?,
                Err(_e) => ClusterConfig::new_local(),
            },
            ClusterMode::Standalone => {
                let cluster_config = parse_arg("cluster_config")?;
                load_config(PathBuf::from(cluster_config))?
            }
            ClusterMode::YARN | ClusterMode::Kubernetes => ClusterConfig::new_local(),
        };

        let (yarn_manager_main_class, worker_process_path, memory_mb, v_cores) = match cluster_mode
        {
            ClusterMode::YARN => match manager_type {
                ManagerType::Coordinator => {
                    let yarn_manager_main_class = parse_arg("yarn_manager_main_class")?;
                    let worker_process_path = parse_arg("worker_process_path")?;

                    let memory_mb = parse_arg("memory_mb")?;
                    let memory_mb = usize::from_str(memory_mb.as_str()).map_err(|_e| {
                        anyhow!("parse `memory_mb`=`{}` to usize error", memory_mb)
                    })?;

                    let v_cores = parse_arg("v_cores")?;
                    let v_cores = usize::from_str(v_cores.as_str())
                        .map_err(|_e| anyhow!("parse `v_cores`=`{}` to usize error", v_cores))?;

                    (
                        yarn_manager_main_class,
                        worker_process_path,
                        memory_mb,
                        v_cores,
                    )
                }
                _ => ("".to_string(), "".to_string(), 0, 0),
            },
            ClusterMode::Kubernetes => match manager_type {
                ManagerType::Coordinator => {
                    let memory_mb = parse_arg("memory_mb")?;
                    let memory_mb = usize::from_str(memory_mb.as_str()).map_err(|_e| {
                        anyhow!("parse `memory_mb`=`{}` to usize error", memory_mb)
                    })?;

                    let v_cores = parse_arg("v_cores")?;
                    let v_cores = usize::from_str(v_cores.as_str())
                        .map_err(|_e| anyhow!("parse `v_cores`=`{}` to usize error", v_cores))?;

                    ("".to_string(), "".to_string(), memory_mb, v_cores)
                }
                _ => ("".to_string(), "".to_string(), 0, 0),
            },
            _ => ("".to_string(), "".to_string(), 0, 0),
        };

        let dashboard_path = match cluster_mode {
            ClusterMode::YARN => {
                let dashboard_path = work_space().join("rlink-dashboard.zip");
                let link_path = dashboard_path.read_link();
                let p = link_path.unwrap_or(dashboard_path);
                p.to_str().unwrap().to_string()
            }
            _ => parse_arg("dashboard_path").unwrap_or_default(),
        };

        let log_config_path = parse_arg("log_config_path")
            .map(|x| Some(x))
            .unwrap_or(None);
        logger::init_log(log_config_path)?;

        let metric_addr = metrics_serve(bind_ip.as_str(), &cluster_mode, &manager_type);

        let coordinator_address = match manager_type {
            ManagerType::Coordinator => "".to_string(),
            _ => parse_arg("coordinator_address")?,
        };

        Ok(Context::new(
            application_name.to_string(),
            application_id,
            task_manager_id,
            bind_ip,
            cluster_mode,
            num_task_managers,
            manager_type,
            cluster_config,
            metric_addr,
            coordinator_address,
            dashboard_path,
            yarn_manager_main_class,
            worker_process_path,
            memory_mb,
            v_cores,
        ))
    }
}

fn metrics_serve(bind_ip: &str, cluster_mode: &ClusterMode, manager_type: &ManagerType) -> String {
    let with_proxy = if cluster_mode.clone() != ClusterMode::Local
        && manager_type.clone() == ManagerType::Coordinator
    {
        true
    } else {
        false
    };

    let addr = crate::metrics::init_metrics2(bind_ip, with_proxy).unwrap();
    format!("http://{}:{}", bind_ip, addr.port())
}
