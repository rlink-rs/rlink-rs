use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::api::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::api::element::Element;
use crate::api::function::OutputFormat;
use crate::api::operator::{DefaultStreamOperator, FunctionCreator, TStreamOperator};
use crate::api::runtime::{OperatorId, TaskId};
use crate::metrics::{register_counter, Tag};
use crate::runtime::worker::checkpoint::submit_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

#[derive(Debug)]
pub(crate) struct SinkRunnable {
    operator_id: OperatorId,
    task_id: TaskId,

    context: Option<RunnableContext>,

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
            task_id: TaskId::default(),
            context: None,
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
            Tag::from(("job_id", self.task_id.job_id.0)),
            Tag::from(("task_number", self.task_id.task_number)),
        ];
        let metric_name = format!("Sink_{}", self.stream_sink.operator_fn.as_ref().name());
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
                match self.stream_sink.fn_creator() {
                    FunctionCreator::System => {
                        // distribution to downstream
                        self.stream_sink
                            .operator_fn
                            .write_element(Element::from(barrier));
                    }
                    FunctionCreator::User => {
                        let snapshot_context = {
                            let context = self.context.as_ref().unwrap();
                            context.checkpoint_context(self.operator_id, barrier.checkpoint_id)
                        };
                        self.checkpoint(snapshot_context);
                    }
                }
            }
            Element::Watermark(watermark) => {
                match self.stream_sink.fn_creator() {
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
                match self.stream_sink.fn_creator() {
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

    fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext) {
        let handle = match self.stream_sink.operator_fn.checkpoint_function() {
            Some(checkpoint) => checkpoint.snapshot_state(&snapshot_context),
            None => CheckpointHandle::default(),
        };

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
