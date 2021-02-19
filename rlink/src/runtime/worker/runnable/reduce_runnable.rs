use std::borrow::BorrowMut;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::api::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::api::element::{Element, Record, StreamStatus};
use crate::api::function::{BaseReduceFunction, KeySelectorFunction};
use crate::api::operator::DefaultStreamOperator;
use crate::api::runtime::{OperatorId, TaskId};
use crate::api::window::{TWindow, Window};
use crate::metrics::{register_counter, Tag};
use crate::runtime::worker::checkpoint::submit_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

#[derive(Debug)]
pub(crate) struct ReduceRunnable {
    operator_id: OperatorId,
    task_id: TaskId,

    context: Option<RunnableContext>,

    stream_key_by: Option<DefaultStreamOperator<dyn KeySelectorFunction>>,
    stream_reduce: DefaultStreamOperator<dyn BaseReduceFunction>,
    next_runnable: Option<Box<dyn Runnable>>,

    // the Record can be operate after this window(include this window's time)
    limited_watermark_window: Window,

    counter: Arc<AtomicU64>,
    expire_counter: Arc<AtomicU64>,
}

impl ReduceRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_key_by: Option<DefaultStreamOperator<dyn KeySelectorFunction>>,
        stream_reduce: DefaultStreamOperator<dyn BaseReduceFunction>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        ReduceRunnable {
            operator_id,
            task_id: TaskId::default(),
            context: None,
            stream_key_by,
            stream_reduce,
            next_runnable,
            limited_watermark_window: Window::default(),
            counter: Arc::new(AtomicU64::new(0)),
            expire_counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Runnable for ReduceRunnable {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().open(context)?;

        self.task_id = context.task_descriptor.task_id;

        self.context = Some(context.clone());

        let fun_context = context.to_fun_context(self.operator_id);
        self.stream_reduce.operator_fn.open(&fun_context)?;
        self.stream_key_by
            .as_mut()
            .map(|s| s.operator_fn.open(&fun_context));

        let tags = vec![
            Tag::from(("job_id", self.task_id.job_id.0)),
            Tag::from(("task_number", self.task_id.task_number)),
        ];
        let fn_name = self.stream_reduce.operator_fn.as_ref().name();

        let metric_name = format!("Reduce_{}", fn_name);
        register_counter(metric_name.as_str(), tags.clone(), self.counter.clone());

        let metric_name = format!("Reduce_Expire_{}", fn_name);
        register_counter(metric_name.as_str(), tags, self.expire_counter.clone());

        info!("ReduceRunnable Opened. task_id={:?}", self.task_id);
        Ok(())
    }

    fn run(&mut self, element: Element) {
        match element {
            Element::Record(mut record) => {
                // Record expiration check
                let min_window_timestamp = self.limited_watermark_window.min_timestamp();
                let acceptable = record
                    .max_location_window()
                    .map(|window| window.min_timestamp() >= min_window_timestamp)
                    .unwrap_or(true);
                if !acceptable {
                    let n = self.expire_counter.fetch_add(1, Ordering::Relaxed);
                    if n & 1048575 == 1 {
                        error!(
                            "expire data. record window={:?}, limit window={:?}",
                            record.min_location_window().unwrap(),
                            self.limited_watermark_window
                        );
                    }
                    return;
                }

                let key = match &self.stream_key_by {
                    Some(stream_key_by) => stream_key_by.operator_fn.get_key(record.borrow_mut()),
                    None => Record::with_capacity(0),
                };

                self.stream_reduce.operator_fn.as_mut().reduce(key, record);

                self.counter.fetch_add(1, Ordering::Relaxed);
            }
            Element::Watermark(watermark) => {
                match watermark.min_location_windows() {
                    Some(min_watermark_window) => {
                        self.limited_watermark_window = min_watermark_window.clone();

                        let drop_events = self
                            .stream_reduce
                            .operator_fn
                            .as_mut()
                            .drop_state(min_watermark_window.min_timestamp());
                        for drop_event in drop_events {
                            self.next_runnable
                                .as_mut()
                                .unwrap()
                                .run(Element::from(drop_event));
                        }
                    }
                    None => {
                        unreachable!("watermark must have window on reduce")
                    }
                }

                // convert watermark to stream_status, and continue post
                let stream_status = StreamStatus::new(watermark.status_timestamp, false);
                self.next_runnable
                    .as_mut()
                    .unwrap()
                    .run(Element::StreamStatus(stream_status));
            }
            Element::Barrier(barrier) => {
                let checkpoint_id = barrier.checkpoint_id;
                let snapshot_context = {
                    let context = self.context.as_ref().unwrap();
                    context.checkpoint_context(self.operator_id, checkpoint_id)
                };
                self.checkpoint(snapshot_context);

                self.next_runnable
                    .as_mut()
                    .unwrap()
                    .run(Element::Barrier(barrier));
            }
            Element::StreamStatus(_) => unreachable!("shouldn't catch `StreamStatus` in reduce"),
        }
    }

    fn close(&mut self) -> anyhow::Result<()> {
        self.stream_key_by.as_mut().map(|s| s.operator_fn.close());
        self.stream_reduce.operator_fn.close()?;
        self.next_runnable.as_mut().unwrap().close()
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext) {
        let handle = self
            .stream_reduce
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
