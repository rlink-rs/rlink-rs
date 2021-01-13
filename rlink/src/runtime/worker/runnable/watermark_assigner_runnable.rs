use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use crate::api::element::Element;
use crate::api::operator::StreamOperator;
use crate::api::runtime::{CheckpointId, OperatorId};
use crate::api::watermark::{Watermark, WatermarkAssigner, MAX_WATERMARK, MIN_WATERMARK};
use crate::metrics::{register_counter, register_gauge, Tag};
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use crate::utils::date_time::timestamp_str;

#[derive(Debug)]
pub(crate) struct WatermarkAssignerRunnable {
    operator_id: OperatorId,
    task_number: u16,
    num_tasks: u16,

    stream_watermark: StreamOperator<dyn WatermarkAssigner>,
    next_runnable: Option<Box<dyn Runnable>>,
    watermark: Watermark,

    watermark_gauge: Arc<AtomicI64>,
    expire_counter: Arc<AtomicU64>,
}

impl WatermarkAssignerRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_watermark: StreamOperator<dyn WatermarkAssigner>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create WatermarkAssignerRunnable");

        WatermarkAssignerRunnable {
            operator_id,
            task_number: 0,
            num_tasks: 0,
            stream_watermark,
            next_runnable,
            watermark: MIN_WATERMARK,
            watermark_gauge: Arc::new(AtomicI64::new(0)),
            expire_counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Runnable for WatermarkAssignerRunnable {
    fn open(&mut self, context: &RunnableContext) {
        self.next_runnable.as_mut().unwrap().open(context);

        self.task_number = context.task_descriptor.task_id.task_number;
        self.num_tasks = context.task_descriptor.task_id.num_tasks;

        let tags = vec![
            Tag(
                "job_id".to_string(),
                context.task_descriptor.task_id.job_id.0.to_string(),
            ),
            Tag(
                "task_number".to_string(),
                context.task_descriptor.task_id.task_number.to_string(),
            ),
        ];
        let fn_name = self.stream_watermark.operator_fn.as_ref().get_name();

        let metric_name = format!("Watermark_{}", fn_name);
        register_gauge(
            metric_name.as_str(),
            tags.clone(),
            self.watermark_gauge.clone(),
        );

        let metric_name = format!("Watermark_Expire_{}", fn_name);
        register_counter(metric_name.as_str(), tags, self.expire_counter.clone());
    }

    fn run(&mut self, mut element: Element) {
        let watermark_assigner = &mut self.stream_watermark.operator_fn;

        if element.is_record() {
            let record = element.as_record_mut();
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
        } else if element.is_stream_status() {
            let stream_status = element.as_stream_status();
            if stream_status.end {
                self.next_runnable
                    .as_mut()
                    .unwrap()
                    .run(Element::new_watermark(
                        self.task_number,
                        self.num_tasks,
                        MAX_WATERMARK.timestamp,
                        stream_status,
                    ));
            } else {
                match watermark_assigner.get_watermark(&element) {
                    Some(watermark) => {
                        debug!("Emit watermark {:?}", timestamp_str(watermark.timestamp));
                        self.watermark = watermark;
                        self.watermark_gauge
                            .store(self.watermark.timestamp as i64, Ordering::Relaxed);
                        let watermark_ele = Element::new_watermark(
                            self.task_number,
                            self.num_tasks,
                            self.watermark.timestamp,
                            stream_status,
                        );

                        self.next_runnable.as_mut().unwrap().run(watermark_ele);
                    }
                    None => {}
                }
            }
        }
    }

    fn close(&mut self) {
        self.next_runnable.as_mut().unwrap().close();
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, _checkpoint_id: CheckpointId) {}
}
