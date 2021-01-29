use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::api::checkpoint::FunctionSnapshotContext;
use crate::api::element::Element;
use crate::api::function::FlatMapFunction;
use crate::api::operator::DefaultStreamOperator;
use crate::api::runtime::OperatorId;
use crate::metrics::{register_counter, Tag};
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

#[derive(Debug)]
pub(crate) struct FlatMapRunnable {
    operator_id: OperatorId,
    stream_map: DefaultStreamOperator<dyn FlatMapFunction>,
    next_runnable: Option<Box<dyn Runnable>>,

    counter: Arc<AtomicU64>,
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
            stream_map,
            next_runnable,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }
}
impl Runnable for FlatMapRunnable {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().open(context)?;

        let fun_context = context.to_fun_context(self.operator_id);
        self.stream_map.operator_fn.open(&fun_context)?;

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
        let metric_name = format!(
            "FlatMap_{}",
            self.stream_map.operator_fn.as_ref().get_name()
        );
        register_counter(metric_name.as_str(), tags, self.counter.clone());

        Ok(())
    }

    fn run(&mut self, element: Element) {
        if element.is_record() {
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

            self.counter.fetch_add(len, Ordering::Relaxed);
        } else {
            self.next_runnable.as_mut().unwrap().run(element);
        }
    }

    fn close(&mut self) -> anyhow::Result<()> {
        self.stream_map.operator_fn.close()?;
        self.next_runnable.as_mut().unwrap().close()
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, _snapshot_context: FunctionSnapshotContext) {}
}
