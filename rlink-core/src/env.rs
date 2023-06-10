use daggy::Dag;

use crate::cmd::CmdFactory;
use crate::job::{JobEdge, JobFactory, JobNode};

#[async_trait]
pub trait Env: Send + Sync {
    async fn set_config(&self, key: &str, value: &str);

    async fn run(
        &self,
        name: &str,
        job_dag: Dag<JobNode, JobEdge>,
        job_factory: Box<dyn JobFactory>,
        cmds: Vec<Box<dyn CmdFactory>>,
    ) -> anyhow::Result<()>;
}
