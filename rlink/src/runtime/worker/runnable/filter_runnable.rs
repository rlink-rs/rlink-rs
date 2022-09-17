use std::borrow::BorrowMut;

use crate::core::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::Element;
use crate::core::function::FilterFunction;
use crate::core::operator::DefaultStreamOperator;
use crate::core::runtime::OperatorId;
use crate::runtime::worker::checkpoint::submit_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

pub(crate) struct FilterRunnable {
    operator_id: OperatorId,
    stream_filter: DefaultStreamOperator<dyn FilterFunction>,
    next_runnable: Option<Box<dyn Runnable>>,

    context: Option<RunnableContext>,
}

impl FilterRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_filter: DefaultStreamOperator<dyn FilterFunction>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create FilterRunnable");

        FilterRunnable {
            operator_id,
            stream_filter,
            next_runnable,
            context: None,
        }
    }
}

#[async_trait]
impl Runnable for FilterRunnable {
    async fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().open(context).await?;

        self.context = Some(context.clone());

        let fun_context = context.to_fun_context(self.operator_id);
        self.stream_filter.operator_fn.open(&fun_context).await?;

        Ok(())
    }

    async fn run(&mut self, mut element: Element) {
        match element.borrow_mut() {
            Element::Record(record) => {
                if self.stream_filter.operator_fn.as_mut().filter(record).await {
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

                self.next_runnable.as_mut().unwrap().run(element).await;
            }
            _ => {
                self.next_runnable.as_mut().unwrap().run(element).await;
            }
        }
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.stream_filter.operator_fn.close().await?;
        self.next_runnable.as_mut().unwrap().close().await
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    async fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext) {
        let handle = self
            .stream_filter
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
        submit_checkpoint(ck).map(|ck| {
            error!(
                "{:?} submit checkpoint error. maybe report channel is full, checkpoint: {:?}",
                snapshot_context.operator_id, ck
            )
        });
    }
}
