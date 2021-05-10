use crate::core::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::Element;
use crate::core::function::FlatMapFunction;
use crate::core::operator::DefaultStreamOperator;
use crate::core::runtime::{OperatorId, TaskId};
use crate::metrics::metric::Counter;
use crate::metrics::register_counter;
use crate::runtime::worker::checkpoint::submit_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use std::borrow::BorrowMut;

#[derive(Debug)]
pub(crate) struct FlatMapRunnable {
    operator_id: OperatorId,
    task_id: TaskId,

    stream_map: DefaultStreamOperator<dyn FlatMapFunction>,
    next_runnable: Option<Box<dyn Runnable>>,

    context: Option<RunnableContext>,

    counter: Counter,
}

impl FlatMapRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_map: DefaultStreamOperator<dyn FlatMapFunction>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create FlatMapRunnable");

        FlatMapRunnable {
            operator_id,
            task_id: TaskId::default(),
            stream_map,
            next_runnable,
            context: None,
            counter: Counter::default(),
        }
    }
}
impl Runnable for FlatMapRunnable {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().open(context)?;

        self.context = Some(context.clone());

        self.task_id = context.task_descriptor.task_id;

        let fun_context = context.to_fun_context(self.operator_id);
        self.stream_map.operator_fn.open(&fun_context)?;

        self.counter = register_counter(
            format!("FlatMap_{}", self.stream_map.operator_fn.as_ref().name()),
            self.task_id.to_tags(),
        );

        Ok(())
    }

    fn run(&mut self, mut element: Element) {
        match element.borrow_mut() {
            Element::Record(_record) => {
                let elements = self
                    .stream_map
                    .operator_fn
                    .as_mut()
                    .flat_map_element(element);

                let mut len = 0;
                for ele in elements {
                    self.next_runnable.as_mut().unwrap().run(ele);
                    len += 1;
                }

                self.counter.fetch_add(len);
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
        self.stream_map.operator_fn.close()?;
        self.next_runnable.as_mut().unwrap().close()
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext) {
        let handle = self
            .stream_map
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
