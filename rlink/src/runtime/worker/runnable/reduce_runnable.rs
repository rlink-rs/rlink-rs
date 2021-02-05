use std::borrow::BorrowMut;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::api::backend::KeyedStateBackend;
use crate::api::checkpoint::{FunctionSnapshotContext, Checkpoint, CheckpointHandle};
use crate::api::element::{Element, Record, StreamStatus};
use crate::api::function::{KeySelectorFunction, ReduceFunction};
use crate::api::operator::DefaultStreamOperator;
use crate::api::properties::SystemProperties;
use crate::api::runtime::{OperatorId, TaskId};
use crate::api::window::{TWindow, Window};
use crate::metrics::{register_counter, Tag};
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use crate::storage::keyed_state::{TWindowState, WindowState};
use crate::utils::date_time::timestamp_str;
use crate::runtime::worker::checkpoint::submit_checkpoint;

#[derive(Debug)]
pub(crate) struct ReduceRunnable {
    operator_id: OperatorId,
    task_id: TaskId,

    context: Option<RunnableContext>,

    parent_parallelism: u16,

    stream_key_by: Option<DefaultStreamOperator<dyn KeySelectorFunction>>,
    stream_reduce: DefaultStreamOperator<dyn ReduceFunction>,
    next_runnable: Option<Box<dyn Runnable>>,

    state: Option<WindowState>, // HashMap<Vec<u8>, Record>, // HashMap<TimeWindow, HashMap<Record, Record>>,

    // the Record can be operate after this window(include this window's time)
    limited_watermark_window: Option<Window>,

    counter: Arc<AtomicU64>,
    expire_counter: Arc<AtomicU64>,
}

impl ReduceRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_key_by: Option<DefaultStreamOperator<dyn KeySelectorFunction>>,
        stream_reduce: DefaultStreamOperator<dyn ReduceFunction>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        ReduceRunnable {
            operator_id,
            task_id: TaskId::default(),
            context: None,
            parent_parallelism: 0,
            stream_key_by,
            stream_reduce,
            next_runnable,
            state: None,
            limited_watermark_window: None,
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

        self.parent_parallelism = context.parent_parallelism();

        // self.watermark_align = Some(WatermarkAlign::new());

        info!("ReduceRunnable Opened. task_id={:?}", self.task_id);

        let state_mode = context
            .application_descriptor
            .coordinator_manager
            .application_properties
            .get_keyed_state_backend()
            .unwrap_or(KeyedStateBackend::Memory);

        self.state = Some(WindowState::new(
            context
                .application_descriptor
                .coordinator_manager
                .application_id
                .clone(),
            context.task_descriptor.task_id.job_id,
            context.task_descriptor.task_id.task_number,
            state_mode,
        ));

        let tags = vec![
            Tag::from(("job_id", self.task_id.job_id.0)),
            Tag::from(("task_number", self.task_id.task_number)),
        ];
        let fn_name = self.stream_reduce.operator_fn.as_ref().name();

        let metric_name = format!("Reduce_{}", fn_name);
        register_counter(metric_name.as_str(), tags.clone(), self.counter.clone());

        let metric_name = format!("Reduce_Expire_{}", fn_name);
        register_counter(metric_name.as_str(), tags, self.expire_counter.clone());

        Ok(())
    }

    fn run(&mut self, element: Element) {
        let state = self.state.as_mut().unwrap();
        match element {
            Element::Record(mut record) => {
                // Record expiration check
                let acceptable = self
                    .limited_watermark_window
                    .as_ref()
                    .map(|limit_window| {
                        record
                            .max_location_windows()
                            .map(|window| window.min_timestamp() >= limit_window.min_timestamp())
                            .unwrap_or(true)
                    })
                    .unwrap_or(true);
                if !acceptable {
                    let n = self.expire_counter.fetch_add(1, Ordering::Relaxed);
                    if n & 1048575 == 1 {
                        error!(
                            "expire data. record window={:?}, limit window={:?}",
                            record.min_location_windows().unwrap(),
                            self.limited_watermark_window.as_ref().unwrap()
                        );
                    }
                    return;
                }

                let key = match &self.stream_key_by {
                    Some(stream_key_by) => stream_key_by.operator_fn.get_key(record.borrow_mut()),
                    None => Record::with_capacity(0),
                };

                let reduce_func = &self.stream_reduce.operator_fn;
                state.merge(key, record, |val1, val2| reduce_func.reduce(val1, val2));

                self.counter.fetch_add(1, Ordering::Relaxed);
            }
            Element::Watermark(watermark) => {
                match watermark.min_location_windows() {
                    Some(min_watermark_window) => {
                        self.limited_watermark_window = Some(min_watermark_window.clone());

                        let mut drop_windows = Vec::new();
                        for window in state.windows() {
                            if window.max_timestamp() <= min_watermark_window.min_timestamp() {
                                drop_windows.push(window.clone());
                                state.drop_window(&window);

                                // info!(
                                //     "drop window [{}/{}]",
                                //     timestamp_str(window.min_timestamp()),
                                //     timestamp_str(window.max_timestamp()),
                                // );
                            }
                        }

                        if drop_windows.len() > 0 {
                            debug!(
                                "check window for drop, trigger watermark={}, drop window size={}",
                                timestamp_str(min_watermark_window.max_timestamp()),
                                drop_windows.len()
                            );

                            drop_windows.sort_by_key(|w| w.max_timestamp());
                            for drop_window in drop_windows {
                                let mut drop_record = Record::new();
                                drop_record.trigger_window = Some(drop_window);

                                self.next_runnable
                                    .as_mut()
                                    .unwrap()
                                    .run(Element::from(drop_record));
                            }
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
        let windows  =self.state.as_ref().unwrap().windows();
        let handle = serde_json::to_string(&windows).unwrap();

        let ck = Checkpoint {
            operator_id: snapshot_context.operator_id,
            task_id: snapshot_context.task_id,
            checkpoint_id: snapshot_context.checkpoint_id,
            handle: CheckpointHandle{ handle },
        };
        submit_checkpoint(ck).map(|ck| {
            error!(
                "{:?} submit checkpoint error. maybe report channel is full, checkpoint: {:?}",
                snapshot_context.operator_id, ck
            )
        });

    }
}
