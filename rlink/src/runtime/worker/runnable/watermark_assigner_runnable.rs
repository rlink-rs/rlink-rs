use std::borrow::BorrowMut;

use crate::core::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::Element;
use crate::core::operator::DefaultStreamOperator;
use crate::core::runtime::{OperatorId, TaskId};
use crate::core::watermark::{
    TimestampAssigner, Watermark, WatermarkGenerator, WatermarkStrategy, MAX_WATERMARK,
    MIN_WATERMARK,
};
use crate::metrics::metric::{Counter, Gauge};
use crate::metrics::{register_counter, register_gauge};
use crate::runtime::worker::checkpoint::submit_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

pub(crate) struct WatermarkAssignerRunnable {
    operator_id: OperatorId,
    task_id: TaskId,

    watermark_generator: Box<dyn WatermarkGenerator>,
    timestamp_assigner: Box<dyn TimestampAssigner>,
    watermark_strategy: DefaultStreamOperator<dyn WatermarkStrategy>,

    next_runnable: Option<Box<dyn Runnable>>,
    watermark: Watermark,

    context: Option<RunnableContext>,

    watermark_gauge: Gauge,
    expire_counter: Counter,
}

impl WatermarkAssignerRunnable {
    pub fn new(
        operator_id: OperatorId,
        mut watermark_strategy: DefaultStreamOperator<dyn WatermarkStrategy>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create WatermarkAssignerRunnable");

        WatermarkAssignerRunnable {
            operator_id,
            task_id: TaskId::default(),
            watermark_generator: watermark_strategy.operator_fn.create_watermark_generator(),
            timestamp_assigner: watermark_strategy.operator_fn.create_timestamp_assigner(),
            watermark_strategy,
            next_runnable,
            watermark: MIN_WATERMARK,
            context: None,
            watermark_gauge: Gauge::default(),
            expire_counter: Counter::default(),
        }
    }

    fn update_watermark_progress(&mut self, watermark: Watermark) {
        if watermark.timestamp > MAX_WATERMARK.timestamp {
            return;
        }

        self.watermark = watermark;
        self.watermark_gauge.store(self.watermark.timestamp as i64);
    }
}

#[async_trait]
impl Runnable for WatermarkAssignerRunnable {
    async fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().open(context).await?;

        self.context = Some(context.clone());

        self.task_id = context.task_descriptor.task_id;

        let fn_name = self.watermark_strategy.operator_fn.as_ref().name();
        self.watermark_gauge =
            register_gauge(format!("Watermark_{}", fn_name), self.task_id.to_tags());

        self.expire_counter = register_counter(
            format!("Watermark_Expire_{}", fn_name),
            self.task_id.to_tags(),
        );

        let fun_context = context.to_fun_context(self.operator_id);
        self.timestamp_assigner.open(&fun_context)?;

        Ok(())
    }

    async fn run(&mut self, mut element: Element) {
        match element.borrow_mut() {
            Element::Record(record) => {
                let timestamp = self.timestamp_assigner.extract_timestamp(record, 0);
                record.timestamp = timestamp;

                let watermark = self
                    .watermark_generator
                    .on_event(record.borrow_mut(), timestamp);

                if record.timestamp < self.watermark.timestamp {
                    let n = self.expire_counter.fetch_add(1);
                    // 8388605 = 8 * 1024 * 1024 -1
                    if n & 8388605 == 1 {
                        warn!(
                            "lost delay data {} < {}",
                            record.timestamp, self.watermark.timestamp
                        );
                    }
                    return;
                }

                self.next_runnable.as_mut().unwrap().run(element).await;

                if let Some(watermark) = watermark {
                    self.update_watermark_progress(watermark);

                    let watermark_ele = Element::new_watermark(self.watermark.timestamp);
                    self.next_runnable
                        .as_mut()
                        .unwrap()
                        .run(watermark_ele)
                        .await;
                }
            }
            Element::StreamStatus(stream_status) => {
                if stream_status.end {
                    let watermark_ele = Element::max_watermark();
                    self.next_runnable
                        .as_mut()
                        .unwrap()
                        .run(watermark_ele)
                        .await;
                } else {
                    let watermark = self
                        .watermark_generator
                        .on_periodic_emit()
                        .unwrap_or(MIN_WATERMARK.clone());
                    self.update_watermark_progress(watermark);

                    let watermark_ele = Element::new_watermark(self.watermark.timestamp);
                    self.next_runnable
                        .as_mut()
                        .unwrap()
                        .run(watermark_ele)
                        .await;
                }

                // must send after the `Watermark`
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
            Element::Watermark(watermark) => {
                error!("unreachable Watermark, {:?}", watermark);
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
            .watermark_strategy
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
