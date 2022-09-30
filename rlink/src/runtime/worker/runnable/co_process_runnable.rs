use futures::StreamExt;
use std::collections::HashMap;

use crate::core::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::Element;
use crate::core::function::CoProcessFunction;
use crate::core::operator::DefaultStreamOperator;
use crate::core::runtime::{JobId, OperatorId};
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

pub(crate) struct CoProcessRunnable {
    operator_id: OperatorId,
    stream_co_process: DefaultStreamOperator<dyn CoProcessFunction>,
    next_runnable: Option<Box<dyn Runnable>>,

    context: Option<RunnableContext>,

    /// key: JobId,
    /// value: DataStream index  
    parent_jobs: HashMap<JobId, usize>,
}

impl CoProcessRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_co_process: DefaultStreamOperator<dyn CoProcessFunction>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create FilterRunnable");

        CoProcessRunnable {
            operator_id,
            stream_co_process,
            next_runnable,
            context: None,
            parent_jobs: HashMap::new(),
        }
    }
}

#[async_trait]
impl Runnable for CoProcessRunnable {
    async fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().open(context).await?;

        self.context = Some(context.clone());

        // let parent_jobs = context.parent_jobs();
        // for index in 0..parent_jobs.len() {
        //     let (job_node, _job_edge) = parent_jobs[index];
        //     self.parent_jobs.insert(job_node.job_id, index);
        //     job_map0.insert(job_node.job_id, index);
        // }

        // find the stream_node of `input_format`
        // the chain: input_format -> connect, so the `connect` is only one parent
        let source_stream_node = &context.job_node().stream_nodes[0];

        for index in 0..source_stream_node.parent_ids.len() {
            let parent_id = source_stream_node.parent_ids[index];
            let parent_job_id = context
                .parent_jobs()
                .iter()
                .find_map(|(node, _)| {
                    node.stream_nodes
                        .iter()
                        .find(|x| x.id == parent_id)
                        .map(|_x| node.job_id)
                })
                .ok_or(anyhow!("co_process_function parent not found"))?;
            self.parent_jobs.insert(parent_job_id, index);
        }

        let fun_context = context.to_fun_context(self.operator_id);
        self.stream_co_process
            .operator_fn
            .open(&fun_context)
            .await?;

        Ok(())
    }

    async fn run(&mut self, element: Element) {
        match element {
            Element::Record(record) => {
                let stream_seq = *self
                    .parent_jobs
                    .get(&record.channel_key.source_task_id.job_id)
                    .expect("parent job not found");

                let mut element_stream = if stream_seq == self.parent_jobs.len() - 1 {
                    self.stream_co_process
                        .operator_fn
                        .as_mut()
                        .process_left(record)
                        .await
                } else {
                    self.stream_co_process
                        .operator_fn
                        .as_mut()
                        .process_right(stream_seq, record)
                        .await
                };

                while let Some(element) = element_stream.next().await {
                    self.next_runnable.as_mut().unwrap().run(element).await;
                }
            }
            Element::Barrier(barrier) => {
                let checkpoint_id = barrier.checkpoint_id;
                let snapshot_context = {
                    let context = self.context.as_ref().unwrap();
                    context.checkpoint_context(self.operator_id, checkpoint_id, None)
                };
                self.checkpoint(snapshot_context).await;

                self.next_runnable
                    .as_mut()
                    .unwrap()
                    .run(Element::Barrier(barrier))
                    .await;
            }
            _ => {
                self.next_runnable.as_mut().unwrap().run(element).await;
            }
        }
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.stream_co_process.operator_fn.close().await?;
        self.next_runnable.as_mut().unwrap().close().await
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    async fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext) {
        let handle = self
            .stream_co_process
            .operator_fn
            .snapshot_state(&snapshot_context)
            .await
            .unwrap_or(CheckpointHandle::default());

        let ck = Checkpoint {
            operator_id: snapshot_context.operator_id,
            task_id: snapshot_context.task_id,
            checkpoint_id: snapshot_context.checkpoint_id,
            completed_checkpoint_id: snapshot_context.completed_checkpoint_id,
            handle,
        };
        snapshot_context.report(ck).map(|ck| {
            error!(
                "{:?} submit checkpoint error. maybe report channel is full, checkpoint: {:?}",
                snapshot_context.operator_id, ck
            )
        });
    }
}
