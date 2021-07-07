use std::borrow::BorrowMut;

use crate::core::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::{Element, Record, StreamStatus};
use crate::core::function::{BaseReduceFunction, KeySelectorFunction};
use crate::core::operator::DefaultStreamOperator;
use crate::core::runtime::{CheckpointId, OperatorId, TaskId};
use crate::core::watermark::MAX_WATERMARK;
use crate::core::window::{TWindow, Window};
use crate::metrics::metric::Counter;
use crate::metrics::register_counter;
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
    completed_checkpoint_id: Option<CheckpointId>,

    counter: Counter,
    expire_counter: Counter,
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
            completed_checkpoint_id: None,
            counter: Counter::default(),
            expire_counter: Counter::default(),
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

        let fn_name = self.stream_reduce.operator_fn.as_ref().name();

        self.counter = register_counter(format!("Reduce_{}", fn_name), self.task_id.to_tags());

        self.expire_counter =
            register_counter(format!("Reduce_Expire_{}", fn_name), self.task_id.to_tags());

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
                    let n = self.expire_counter.fetch_add(1);
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

                self.counter.fetch_add(1);
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
            Element::Barrier(mut barrier) => {
                let checkpoint_id = barrier.checkpoint_id;
                let snapshot_context = {
                    let context = self.context.as_ref().unwrap();
                    context.checkpoint_context(self.operator_id, checkpoint_id, None)
                };
                self.checkpoint(snapshot_context);

                if let Some(completed_checkpoint_id) = self.completed_checkpoint_id {
                    barrier.set_completed_checkpoint_id(completed_checkpoint_id);
                }
                self.next_runnable
                    .as_mut()
                    .unwrap()
                    .run(Element::Barrier(barrier));
            }
            Element::StreamStatus(stream_status) => {
                if !stream_status.end {
                    unreachable!("shouldn't catch `StreamStatus` in reduce");
                }

                // clean all reduce state
                let drop_events = self
                    .stream_reduce
                    .operator_fn
                    .as_mut()
                    .drop_state(MAX_WATERMARK.timestamp);
                for drop_event in drop_events {
                    self.next_runnable
                        .as_mut()
                        .unwrap()
                        .run(Element::from(drop_event));
                }

                self.next_runnable
                    .as_mut()
                    .unwrap()
                    .run(Element::StreamStatus(stream_status));
            }
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

        let fn_handle = ReduceCheckpointHandle::from(handle.handle.as_str());
        self.completed_checkpoint_id = fn_handle.completed_checkpoint_id;

        let ck = Checkpoint {
            operator_id: snapshot_context.operator_id,
            task_id: snapshot_context.task_id,
            checkpoint_id: snapshot_context.checkpoint_id,
            completed_checkpoint_id: self.completed_checkpoint_id,
            handle: CheckpointHandle {
                handle: fn_handle.to_windows_string(),
            },
        };
        submit_checkpoint(ck).map(|ck| {
            error!(
                "{:?} submit checkpoint error. maybe report channel is full, checkpoint: {:?}",
                snapshot_context.operator_id, ck
            )
        });
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct ReduceCheckpointHandle {
    #[serde(rename = "c_ck")]
    completed_checkpoint_id: Option<CheckpointId>,
    #[serde(rename = "windows")]
    current_windows: Vec<Window>,
}

impl ReduceCheckpointHandle {
    pub fn new(
        completed_checkpoint_id: Option<CheckpointId>,
        current_windows: Vec<Window>,
    ) -> Self {
        ReduceCheckpointHandle {
            completed_checkpoint_id,
            current_windows,
        }
    }

    pub fn to_windows_string(&self) -> String {
        serde_json::to_string(&self.current_windows).unwrap()
    }

    pub fn into_windows(self) -> Vec<Window> {
        self.current_windows
    }
}

impl ToString for ReduceCheckpointHandle {
    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

impl<'a> From<&'a str> for ReduceCheckpointHandle {
    fn from(handle: &'a str) -> Self {
        if handle.is_empty() {
            ReduceCheckpointHandle::default()
        } else if handle.starts_with("[") {
            let windows: Vec<Window> = serde_json::from_str(handle).unwrap();
            ReduceCheckpointHandle {
                completed_checkpoint_id: None,
                current_windows: windows,
            }
        } else {
            serde_json::from_str(handle).unwrap()
        }
    }
}
