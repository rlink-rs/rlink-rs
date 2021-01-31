use std::borrow::BorrowMut;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::api::checkpoint::FunctionSnapshotContext;
use crate::api::element::{Element, Partition};
use crate::api::function::KeySelectorFunction;
use crate::api::operator::DefaultStreamOperator;
use crate::api::runtime::{OperatorId, TaskId};
use crate::metrics::{register_counter, Tag};
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use crate::utils;

#[derive(Debug)]
pub(crate) struct KeyByRunnable {
    operator_id: OperatorId,
    task_id: TaskId,

    stream_key_by: DefaultStreamOperator<dyn KeySelectorFunction>,
    next_runnable: Option<Box<dyn Runnable>>,
    partition_size: u16,

    counter: Arc<AtomicU64>,
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
            counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Runnable for KeyByRunnable {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().open(context)?;

        self.task_id = context.task_descriptor.task_id;

        let fun_context = context.to_fun_context(self.operator_id);
        self.stream_key_by.operator_fn.open(&fun_context)?;

        // todo set self.partition_size = Reduce.partition
        self.partition_size = context.child_parallelism() as u16;

        let tags = vec![
            Tag::from(("job_id", self.task_id.job_id.0)),
            Tag::from(("task_number", self.task_id.task_number)),
        ];
        let metric_name = format!("KeyBy_{}", self.stream_key_by.operator_fn.as_ref().name());
        register_counter(metric_name.as_str(), tags, self.counter.clone());

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

                self.counter.fetch_add(1, Ordering::Relaxed);
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

    fn checkpoint(&mut self, _snapshot_context: FunctionSnapshotContext) {}
}
