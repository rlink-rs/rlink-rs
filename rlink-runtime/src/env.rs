use std::sync::Arc;

use daggy::Dag;
use dashmap::DashMap;
use rlink_core::cmd::CmdFactory;
use rlink_core::env::Env;
use rlink_core::job::{JobEdge, JobFactory, JobNode};
use rlink_core::properties::Properties;
use tokio::sync::Mutex;

use crate::cmd::Cmd;
use crate::dag::dag_manager::DagManager;
use crate::metrics::install_recorder;
use crate::runtime::cluster::run_task;
use crate::timer::{start_window_timer, WindowTimer};
use crate::utils::context;
use crate::utils::panic::panic_notify;

#[derive(Clone)]
pub struct RuntimeEnv {
    jobs: Arc<DashMap<String, Arc<ApplicationEnv>>>,
    properties: Arc<Mutex<Properties>>,
}

impl RuntimeEnv {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(Default::default()),
            properties: Arc::new(Mutex::new(Default::default())),
        }
    }
}

#[async_trait]
impl Env for RuntimeEnv {
    async fn set_config(&self, key: &str, value: &str) {
        let mut properties = self.properties.lock().await;
        properties.set_string(key.to_string(), value.to_string());
    }

    async fn run(
        &self,
        name: &str,
        job_dag: Dag<JobNode, JobEdge>,
        job_factory: Box<dyn JobFactory>,
        cmd_factories: Vec<Box<dyn CmdFactory>>,
    ) -> anyhow::Result<()> {
        panic_notify();

        let args = Arc::new(context::parse_node_arg()?);
        info!("Context: {:?}", args);

        install_recorder(args.task_manager_id.as_str()).await?;

        let dag_manager = DagManager::new(job_dag, job_factory).unwrap();
        // run_task(Arc::new(context), dag_manager).await;
        let properties = {
            let properties = self.properties.lock().await;
            properties.clone()
        };

        let cmd_factories = cmd_factories.into_iter().map(|c| Arc::new(c)).collect();
        let window_timer = start_window_timer().await;
        let application_env = Arc::new(ApplicationEnv::new(
            name.to_string(),
            properties,
            dag_manager,
            cmd_factories,
            window_timer,
        ));
        self.jobs.insert(name.to_string(), application_env.clone());

        run_task(args, application_env).await
    }
}

#[derive(Clone)]
pub struct ApplicationEnv {
    application_name: String,
    properties: Arc<Properties>,
    dag_manager: DagManager,
    cmd: Arc<Cmd>,
}

impl ApplicationEnv {
    pub fn new(
        application_name: String,
        properties: Properties,
        dag_manager: DagManager,
        cmd_factories: Vec<Arc<Box<dyn CmdFactory>>>,
        window_timer: WindowTimer,
    ) -> Self {
        Self {
            application_name,
            properties: Arc::new(properties),
            dag_manager,
            cmd: Arc::new(Cmd::new(cmd_factories, window_timer)),
        }
    }
    pub fn application_name(&self) -> &str {
        &self.application_name
    }
    pub fn properties(&self) -> Arc<Properties> {
        self.properties.clone()
    }
    pub fn dag_manager(&self) -> DagManager {
        self.dag_manager.clone()
    }
    pub fn cmd(&self) -> &Cmd {
        &self.cmd
    }
}
