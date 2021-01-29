use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::api::checkpoint::{Checkpoint, CheckpointHandle};
use crate::api::element::Element;
use crate::api::function::OutputFormat;
use crate::api::operator::{DefaultStreamOperator, FunctionCreator, TStreamOperator};
use crate::api::runtime::{CheckpointId, OperatorId, TaskId};
use crate::metrics::{register_counter, Tag};
use crate::runtime::worker::checkpoint::submit_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

#[derive(Debug)]
pub(crate) struct SinkRunnable {
    operator_id: OperatorId,
    context: Option<RunnableContext>,

    task_id: TaskId,

    stream_sink: DefaultStreamOperator<dyn OutputFormat>,

    counter: Arc<AtomicU64>,
}

impl SinkRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_sink: DefaultStreamOperator<dyn OutputFormat>,
    ) -> Self {
        SinkRunnable {
            operator_id,
            context: None,
            task_id: TaskId::default(),
            stream_sink,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Runnable for SinkRunnable {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.context = Some(context.clone());

        self.task_id = context.task_descriptor.task_id;

        info!(
            "SinkRunnable Opened. operator_id={:?}, task_id={:?}",
            self.operator_id, self.task_id
        );

        let fun_context = context.to_fun_context(self.operator_id);
        self.stream_sink.operator_fn.open(&fun_context)?;

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
        let metric_name = format!("Sink_{}", self.stream_sink.operator_fn.as_ref().get_name());
        register_counter(metric_name.as_str(), tags, self.counter.clone());

        Ok(())
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

    fn close(&mut self) -> anyhow::Result<()> {
        self.stream_sink.operator_fn.close()?;
        Ok(())
    }

    fn set_next_runnable(&mut self, _next_runnable: Option<Box<dyn Runnable>>) {
        unimplemented!()
    }

    fn checkpoint(&mut self, checkpoint_id: CheckpointId) {
        let context = {
            let context = self.context.as_ref().unwrap();
            context.get_checkpoint_context(self.operator_id, checkpoint_id)
        };

        let ck = match self.stream_sink.operator_fn.get_checkpoint() {
            Some(checkpoint) => {
                let ck_handle = checkpoint.snapshot_state(&context);
                Checkpoint {
                    operator_id: context.operator_id,
                    task_id: self.task_id,
                    checkpoint_id,
                    handle: ck_handle,
                }
            }
            None => Checkpoint {
                operator_id: context.operator_id,
                task_id: self.task_id,
                checkpoint_id,
                handle: CheckpointHandle::default(),
            },
        };

        submit_checkpoint(ck).map(|ck| {
            error!(
                "{:?} report checkpoint error. maybe report channel is full, checkpoint: {:?}",
                self.operator_id, ck
            )
        });
    }
}
