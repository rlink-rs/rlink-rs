use crate::api::element::Element;
use crate::api::function::MapFunction;
use crate::api::operator::StreamOperator;
use crate::metrics::{register_counter, Tag};
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct MapRunnable {
    stream_map: StreamOperator<dyn MapFunction>,
    next_runnable: Option<Box<dyn Runnable>>,

    counter: Arc<AtomicU64>,
}

impl MapRunnable {
    pub fn new(
        stream_map: StreamOperator<dyn MapFunction>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create MapRunnable");

        MapRunnable {
            stream_map,
            next_runnable,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }
}
impl Runnable for MapRunnable {
    fn open(&mut self, context: &RunnableContext) {
        self.next_runnable.as_mut().unwrap().open(context);

        let fun_context = context.to_fun_context();
        self.stream_map.operator_fn.open(&fun_context);

        let tags = vec![
            Tag(
                "chain_id".to_string(),
                context.task_descriptor.chain_id.to_string(),
            ),
            Tag(
                "partition_num".to_string(),
                context.task_descriptor.task_number.to_string(),
            ),
        ];
        let metric_name = format!("Map_{}", self.stream_map.operator_fn.as_ref().get_name());
        register_counter(metric_name.as_str(), tags, self.counter.clone());
    }

    fn run(&mut self, mut element: Element) {
        if element.is_record() {
            let records = self
                .stream_map
                .operator_fn
                .as_mut()
                .map(element.as_record_mut());
            let len = records.len() as u64;
            for record in records {
                self.next_runnable
                    .as_mut()
                    .unwrap()
                    .run(Element::Record(record));
            }

            self.counter.fetch_add(len, Ordering::Relaxed);
        } else {
            self.next_runnable.as_mut().unwrap().run(element);
        }
    }

    fn close(&mut self) {
        self.stream_map.operator_fn.close();
        self.next_runnable.as_mut().unwrap().close();
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, _checkpoint_id: u64) {}
}
