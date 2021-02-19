use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use crate::api::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::api::element::Element;
use crate::api::operator::DefaultStreamOperator;
use crate::api::runtime::{OperatorId, TaskId};
use crate::api::watermark::{Watermark, WatermarkAssigner, MAX_WATERMARK, MIN_WATERMARK};
use crate::metrics::{register_counter, register_gauge, Tag};
use crate::runtime::worker::checkpoint::submit_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use crate::utils::date_time::timestamp_str;
use std::borrow::BorrowMut;

#[derive(Debug)]
pub(crate) struct WatermarkAssignerRunnable {
    operator_id: OperatorId,
    task_id: TaskId,

    stream_watermark: DefaultStreamOperator<dyn WatermarkAssigner>,
    next_runnable: Option<Box<dyn Runnable>>,
    watermark: Watermark,

    context: Option<RunnableContext>,

    watermark_gauge: Arc<AtomicI64>,
    expire_counter: Arc<AtomicU64>,
}

impl WatermarkAssignerRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_watermark: DefaultStreamOperator<dyn WatermarkAssigner>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create WatermarkAssignerRunnable");

        WatermarkAssignerRunnable {
            operator_id,
            task_id: TaskId::default(),
            stream_watermark,
            next_runnable,
            watermark: MIN_WATERMARK,
            context: None,
            watermark_gauge: Arc::new(AtomicI64::new(0)),
            expire_counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Runnable for WatermarkAssignerRunnable {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().open(context)?;

        self.context = Some(context.clone());

        self.task_id = context.task_descriptor.task_id;

        let tags = vec![
            Tag::from(("job_id", self.task_id.job_id.0)),
            Tag::from(("task_number", self.task_id.task_number)),
        ];
        let fn_name = self.stream_watermark.operator_fn.as_ref().name();

        let metric_name = format!("Watermark_{}", fn_name);
        register_gauge(
            metric_name.as_str(),
            tags.clone(),
            self.watermark_gauge.clone(),
        );

        let metric_name = format!("Watermark_Expire_{}", fn_name);
        register_counter(metric_name.as_str(), tags, self.expire_counter.clone());

        Ok(())
    }

    fn run(&mut self, mut element: Element) {
        let watermark_assigner = &mut self.stream_watermark.operator_fn;

        match element.borrow_mut() {
            Element::Record(record) => {
                record.timestamp = watermark_assigner.extract_timestamp(record, 0);

                if record.timestamp < self.watermark.timestamp {
                    let n = self.expire_counter.fetch_add(1, Ordering::Relaxed);
                    // 8388605 = 8 * 1024 * 1024 -1
                    if n & 8388605 == 1 {
                        warn!(
                            "lost delay data {} < {}",
                            record.timestamp, self.watermark.timestamp
                        );
                    }
                    return;
                }
                self.next_runnable.as_mut().unwrap().run(element);
            }
            Element::StreamStatus(stream_status) => {
                if stream_status.end {
                    self.next_runnable
                        .as_mut()
                        .unwrap()
                        .run(Element::new_watermark(
                            self.task_id.task_number,
                            self.task_id.num_tasks,
                            MAX_WATERMARK.timestamp,
                            stream_status,
                        ));
                } else {
                    match watermark_assigner.watermark(stream_status) {
                        Some(watermark) => {
                            debug!("Emit watermark {:?}", timestamp_str(watermark.timestamp));
                            self.watermark = watermark;
                            self.watermark_gauge
                                .store(self.watermark.timestamp as i64, Ordering::Relaxed);
                            let watermark_ele = Element::new_watermark(
                                self.task_id.task_number,
                                self.task_id.num_tasks,
                                self.watermark.timestamp,
                                stream_status,
                            );

                            self.next_runnable.as_mut().unwrap().run(watermark_ele);
                        }
                        None => {}
                    }
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
                error!("unreachable element");
                self.next_runnable.as_mut().unwrap().run(element);
            }
        }
    }

    fn close(&mut self) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().close()
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext) {
        let handle = self
            .stream_watermark
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
