use crate::core::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::{Element, Partition};
use crate::core::function::OutputFormat;
use crate::core::operator::{DefaultStreamOperator, FnOwner, TStreamOperator};
use crate::core::runtime::{OperatorId, TaskId};
use crate::dag::job_graph::JobEdge;
use crate::metrics::metric::Counter;
use crate::metrics::register_counter;
use crate::runtime::worker::checkpoint::submit_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

pub(crate) struct SinkRunnable {
    operator_id: OperatorId,
    task_id: TaskId,
    // child_target_id: TaskId,
    child_parallelism: u16,

    context: Option<RunnableContext>,

    stream_sink: DefaultStreamOperator<dyn OutputFormat>,

    counter: Counter,
}

impl SinkRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_sink: DefaultStreamOperator<dyn OutputFormat>,
    ) -> Self {
        SinkRunnable {
            operator_id,
            task_id: TaskId::default(),
            // child_target_id: TaskId::default(),
            child_parallelism: 0,
            context: None,
            stream_sink,
            counter: Counter::default(),
        }
    }
}

impl Runnable for SinkRunnable {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.context = Some(context.clone());

        self.task_id = context.task_descriptor.task_id;
        let child_jobs = context.child_jobs();
        self.child_parallelism = if child_jobs.len() > 1 {
            unimplemented!()
        } else if child_jobs.len() == 1 {
            let (child_job_node, child_job_edge) = &child_jobs[0];
            match child_job_edge {
                JobEdge::Forward => 0,
                JobEdge::ReBalance => child_job_node.parallelism,
            }
        } else {
            0
        };

        info!(
            "SinkRunnable Opened. operator_id={:?}, task_id={:?}, child_parallelism={}",
            self.operator_id, self.task_id, self.child_parallelism
        );

        let fun_context = context.to_fun_context(self.operator_id);
        self.stream_sink.operator_fn.open(&fun_context)?;

        self.counter = register_counter(
            format!("Sink_{}", self.stream_sink.operator_fn.as_ref().name()),
            self.task_id.to_tags(),
        );

        Ok(())
    }

    fn run(&mut self, element: Element) {
        match element {
            Element::Record(record) => {
                self.stream_sink
                    .operator_fn
                    .write_element(Element::Record(record));

                self.counter.fetch_add(1);
            }
            _ => {
                if element.is_barrier() {
                    let snapshot_context = {
                        let checkpoint_id = element.as_barrier().checkpoint_id;
                        let completed_checkpoint_id =
                            element.as_barrier().completed_checkpoint_id();
                        let context = self.context.as_ref().unwrap();
                        context.checkpoint_context(
                            self.operator_id,
                            checkpoint_id,
                            completed_checkpoint_id,
                        )
                    };
                    self.checkpoint(snapshot_context);
                }

                match self.stream_sink.fn_owner() {
                    FnOwner::System => {
                        // distribution to downstream
                        if self.child_parallelism > 0 {
                            for index in 0..self.child_parallelism {
                                let mut ele = element.clone();
                                ele.set_partition(index);
                                debug!("downstream barrier: {}", index);

                                self.stream_sink.operator_fn.write_element(ele);
                            }
                        } else {
                            debug!("downstream barrier");
                            self.stream_sink.operator_fn.write_element(element);
                        }
                    }
                    FnOwner::User => {}
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
        let handle = self
            .stream_sink
            .operator_fn
            .snapshot_state(&snapshot_context)
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
