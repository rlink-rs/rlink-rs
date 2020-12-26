use crate::api::element::Element;
use crate::api::operator::{FunctionCreator, StreamOperator, TStreamOperator};
use crate::api::output::OutputFormat;
use crate::metrics::{register_counter, Tag};
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct SinkRunnable {
    task_number: u16,
    num_tasks: u16,

    stream_sink: StreamOperator<dyn OutputFormat>,

    counter: Arc<AtomicU64>,
}

impl SinkRunnable {
    pub fn new(stream_sink: StreamOperator<dyn OutputFormat>) -> Self {
        SinkRunnable {
            task_number: 0,
            num_tasks: 0,
            stream_sink,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Runnable for SinkRunnable {
    fn open(&mut self, context: &RunnableContext) {
        self.task_number = context.task_descriptor.task_number;
        self.num_tasks = context.task_descriptor.num_tasks;

        info!(
            "SinkRunnable Opened. task_number={}, num_tasks={}",
            self.task_number, self.num_tasks
        );

        let fun_context = context.to_fun_context();
        self.stream_sink.operator_fn.open(&fun_context);

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
        let metric_name = format!("Sink_{}", self.stream_sink.operator_fn.as_ref().get_name());
        register_counter(metric_name.as_str(), tags, self.counter.clone());
    }

    fn run(&mut self, element: Element) {
        match element {
            Element::Record(record) => {
                self.stream_sink
                    .operator_fn
                    .write_element(Element::Record(record));

                self.counter.fetch_add(1, Ordering::Relaxed);
            }
            Element::Barrier(barrier) => {
                match self.stream_sink.get_fn_creator() {
                    FunctionCreator::System => {
                        // distribution to downstream
                        self.stream_sink
                            .operator_fn
                            .write_element(Element::from(barrier));
                    }
                    FunctionCreator::User => {
                        self.checkpoint(barrier.checkpoint_id);
                    }
                }
            }
            Element::Watermark(watermark) => {
                match self.stream_sink.get_fn_creator() {
                    FunctionCreator::System => {
                        // distribution to downstream
                        self.stream_sink
                            .operator_fn
                            .write_element(Element::from(watermark));
                    }
                    FunctionCreator::User => {
                        // nothing to do
                    }
                }
            }
            Element::StreamStatus(stream_status) => {
                match self.stream_sink.get_fn_creator() {
                    FunctionCreator::System => {
                        // distribution to downstream
                        self.stream_sink
                            .operator_fn
                            .write_element(Element::from(stream_status));
                    }
                    FunctionCreator::User => {
                        // nothing to do
                    }
                }
            }
        }
    }

    fn close(&mut self) {
        self.stream_sink.operator_fn.close();
    }

    fn set_next_runnable(&mut self, _next_runnable: Option<Box<dyn Runnable>>) {
        unimplemented!()
    }

    fn checkpoint(&mut self, _checkpoint_id: u64) {
        self.stream_sink.operator_fn.prepare_commit();
    }
}
