use crate::api::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::api::element::Element;
use crate::api::function::FilterFunction;
use crate::api::operator::DefaultStreamOperator;
use crate::api::runtime::OperatorId;
use crate::runtime::worker::checkpoint::submit_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use std::borrow::BorrowMut;

#[derive(Debug)]
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

impl Runnable for FilterRunnable {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().open(context)?;

        self.context = Some(context.clone());

        let fun_context = context.to_fun_context(self.operator_id);
        self.stream_filter.operator_fn.open(&fun_context)?;

        Ok(())
    }

    fn run(&mut self, mut element: Element) {
        match element.borrow_mut() {
            Element::Record(record) => {
                if self.stream_filter.operator_fn.as_mut().filter(record) {
                    self.next_runnable.as_mut().unwrap().run(element);
                }
            }
            Element::Barrier(barrier) => {
                let checkpoint_id = barrier.checkpoint_id;
                let snapshot_context = {
                    let context = self.context.as_ref().unwrap();
                    context.checkpoint_context(self.operator_id, checkpoint_id)
                };
                self.checkpoint(snapshot_context);

                self.next_runnable.as_mut().unwrap().run(element);
            }
            _ => {
                self.next_runnable.as_mut().unwrap().run(element);
            }
        }
    }

    fn close(&mut self) -> anyhow::Result<()> {
        self.stream_filter.operator_fn.close()?;
        self.next_runnable.as_mut().unwrap().close()
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext) {
        let handle = self
            .stream_filter
            .operator_fn
            .snapshot_state(&snapshot_context)
            .unwrap_or(CheckpointHandle::default());

        let ck = Checkpoint {
            operator_id: snapshot_context.operator_id,
            task_id: snapshot_context.task_id,
            checkpoint_id: snapshot_context.checkpoint_id,
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
