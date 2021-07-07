use std::borrow::BorrowMut;

use crate::core::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::{Element, Partition};
use crate::core::function::KeySelectorFunction;
use crate::core::operator::DefaultStreamOperator;
use crate::core::runtime::{OperatorId, TaskId};
use crate::metrics::metric::Counter;
use crate::metrics::register_counter;
use crate::runtime::worker::checkpoint::submit_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use crate::utils;

#[derive(Debug)]
pub(crate) struct KeyByRunnable {
    operator_id: OperatorId,
    task_id: TaskId,

    stream_key_by: DefaultStreamOperator<dyn KeySelectorFunction>,
    next_runnable: Option<Box<dyn Runnable>>,
    partition_size: u16,

    context: Option<RunnableContext>,

    counter: Counter,
}

impl KeyByRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_key_by: DefaultStreamOperator<dyn KeySelectorFunction>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        KeyByRunnable {
            operator_id,
            task_id: TaskId::default(),
            stream_key_by,
            next_runnable,
            partition_size: 0,
            context: None,
            counter: Counter::default(),
        }
    }
}

impl Runnable for KeyByRunnable {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().open(context)?;

        self.context = Some(context.clone());

        self.task_id = context.task_descriptor.task_id;

        let fun_context = context.to_fun_context(self.operator_id);
        self.stream_key_by.operator_fn.open(&fun_context)?;

        // todo set self.partition_size = Reduce.partition
        self.partition_size = context.child_parallelism() as u16;

        self.counter = register_counter(
            format!("KeyBy_{}", self.stream_key_by.operator_fn.as_ref().name()),
            self.task_id.to_tags(),
        );

        Ok(())
    }

    fn run(&mut self, mut element: Element) {
        match element.borrow_mut() {
            Element::Record(record) => {
                let key_row = self
                    .stream_key_by
                    .operator_fn
                    .as_mut()
                    .get_key(record.borrow_mut());

                let hash_code = utils::hash::hash_code(key_row.values.as_slice()).unwrap_or(0);
                let partition_num = hash_code % self.partition_size as u32;
                // info!(
                //     "partition: {}, hash code: {}, partition_size: {}",
                //     partition_num,
                //     hash_code,
                //     self.partition_size,
                // );
                record.set_partition(partition_num as u16);

                self.next_runnable.as_mut().unwrap().run(element);

                self.counter.fetch_add(1);
            }
            Element::Barrier(barrier) => {
                let checkpoint_id = barrier.checkpoint_id;
                let snapshot_context = {
                    let context = self.context.as_ref().unwrap();
                    context.checkpoint_context(self.operator_id, checkpoint_id, None)
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
        self.stream_key_by.operator_fn.close()?;
        self.next_runnable.as_mut().unwrap().close()
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext) {
        let handle = self
            .stream_key_by
            .operator_fn
            .snapshot_state(&snapshot_context)
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
