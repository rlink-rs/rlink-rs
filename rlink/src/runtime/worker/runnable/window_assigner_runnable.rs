use std::borrow::BorrowMut;

use crate::core::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::Element;
use crate::core::operator::DefaultStreamOperator;
use crate::core::runtime::OperatorId;
use crate::core::window::{WindowAssigner, WindowAssignerContext};
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

pub(crate) struct WindowAssignerRunnable {
    operator_id: OperatorId,
    stream_window: DefaultStreamOperator<dyn WindowAssigner>,
    next_runnable: Option<Box<dyn Runnable>>,

    context: Option<RunnableContext>,
}

impl WindowAssignerRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_window: DefaultStreamOperator<dyn WindowAssigner>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create WindowAssignerRunnable");

        WindowAssignerRunnable {
            operator_id,
            stream_window,
            next_runnable,
            context: None,
        }
    }
}

#[async_trait]
impl Runnable for WindowAssignerRunnable {
    async fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().open(context).await?;
        info!(
            "WindowAssignerRunnable({}) opened",
            self.stream_window.operator_fn.name()
        );

        self.context = Some(context.clone());

        Ok(())
    }

    async fn run(&mut self, mut element: Element) {
        match element.borrow_mut() {
            Element::Record(record) => {
                let windows = self
                    .stream_window
                    .operator_fn
                    .assign_windows(record.timestamp, WindowAssignerContext {});
                record.set_location_windows(windows);

                self.next_runnable.as_mut().unwrap().run(element).await;
            }
            Element::Watermark(watermark) => {
                let windows = self
                    .stream_window
                    .operator_fn
                    .assign_windows(watermark.timestamp, WindowAssignerContext {});
                watermark.set_location_windows(windows);

                self.next_runnable.as_mut().unwrap().run(element).await;
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
            Element::StreamStatus(_stream_status) => {
                // error!("unreachable element");
                self.next_runnable.as_mut().unwrap().run(element).await;
            }
        }
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().close().await
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    async fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext) {
        let handle = self
            .stream_window
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
