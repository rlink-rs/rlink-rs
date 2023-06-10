use rlink_core::job::TaskContext;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub struct JobTask {
    task_context: Arc<Box<dyn TaskContext>>,
}

impl JobTask {
    fn new(task_context: Arc<Box<dyn TaskContext>>) -> Self {
        JobTask { task_context }
    }

    pub(crate) async fn run_task(task_context: Arc<Box<dyn TaskContext>>) -> JoinHandle<()> {
        let worker_task = JobTask::new(task_context);
        tokio::spawn(async move {
            // todo error handle
            worker_task.run().await.unwrap();
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let task_id = self.task_context.task_id();
        let mut job = self.task_context.create_job(task_id.job_id).await;

        info!("open job task {:?}", task_id);
        job.open(self.task_context.as_ref()).await?;

        info!("run job task {:?}", task_id);
        job.run().await;

        info!("close job task {:?}", task_id);
        job.close().await
    }
}
