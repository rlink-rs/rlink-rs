use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::api::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::api::element::{Barrier, Element};
use crate::api::function::{InputFormat, InputSplit};
use crate::api::operator::{DefaultStreamOperator, FunctionCreator, TStreamOperator};
use crate::api::runtime::{CheckpointId, OperatorId, TaskId};
use crate::channel::named_channel;
use crate::channel::sender::ChannelSender;
use crate::dag::job_graph::JobEdge;
use crate::metrics::Tag;
use crate::runtime::timer::TimerChannel;
use crate::runtime::worker::checkpoint::submit_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

#[derive(Debug)]
pub(crate) struct SourceRunnable {
    operator_id: OperatorId,
    context: Option<RunnableContext>,

    task_id: TaskId,

    stream_source: DefaultStreamOperator<dyn InputFormat>,
    next_runnable: Option<Box<dyn Runnable>>,

    stream_status_timer: Option<TimerChannel>,
    checkpoint_timer: Option<TimerChannel>,

    barrier_align: Option<BarrierAlign>,
}

impl SourceRunnable {
    pub fn new(
        operator_id: OperatorId,
        _input_split: InputSplit, // todo remove?
        stream_source: DefaultStreamOperator<dyn InputFormat>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        SourceRunnable {
            operator_id,
            context: None,
            task_id: TaskId::default(),

            stream_source,
            next_runnable,

            stream_status_timer: None,
            checkpoint_timer: None,

            barrier_align: None,
        }
    }

    fn poll_input_element(&mut self, sender: ChannelSender<Element>, running: Arc<AtomicBool>) {
        let iterator = self.stream_source.operator_fn.element_iter();
        crate::utils::thread::spawn("poll_input_element", move || {
            match SourceRunnable::poll_input_element0(iterator, sender, running) {
                Ok(_) => {}
                Err(e) => panic!("poll_input_element thread error. {}", e),
            }
        });
    }

    fn poll_input_element0(
        iterator: Box<dyn Iterator<Item = Element> + Send>,
        sender: ChannelSender<Element>,
        running: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        for record in iterator {
            sender.send(record).map_err(|e| anyhow!(e))?;
        }

        running.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn poll_stream_status(&mut self, sender: ChannelSender<Element>, running: Arc<AtomicBool>) {
        let stream_status_timer = self.stream_status_timer.as_ref().unwrap().clone();
        crate::utils::thread::spawn("poll_stream_status", move || {
            match SourceRunnable::poll_stream_status0(stream_status_timer, sender, running) {
                Ok(_) => {}
                Err(e) => panic!("poll_stream_status thread error. {}", e),
            }
        });
    }

    fn poll_stream_status0(
        stream_status_timer: TimerChannel,
        sender: ChannelSender<Element>,
        running: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        loop {
            let running = running.load(Ordering::Relaxed);
            let window_time = stream_status_timer.recv().map_err(|e| anyhow!(e))?;

            let stream_status = Element::new_stream_status(window_time, !running);
            sender.send(stream_status).map_err(|e| anyhow!(e))?;

            if !running {
                info!("StreamStatus WindowTimer stop");
                // break;
            }
        }
    }

    fn poll_checkpoint(&mut self, sender: ChannelSender<Element>, running: Arc<AtomicBool>) {
        let checkpoint_timer = self.checkpoint_timer.as_ref().unwrap().clone();
        crate::utils::thread::spawn("poll_checkpoint", move || {
            match SourceRunnable::poll_checkpoint0(checkpoint_timer, sender, running) {
                Ok(_) => {}
                Err(e) => panic!("poll_checkpoint thread error. {}", e),
            }
        });
    }

    fn poll_checkpoint0(
        checkpoint_timer: TimerChannel,
        sender: ChannelSender<Element>,
        running: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        loop {
            let running = running.load(Ordering::Relaxed);

            let window_time = checkpoint_timer.recv().map_err(|e| anyhow!(e))?;

            let barrier = Element::new_barrier(CheckpointId(window_time));
            sender.send(barrier).map_err(|e| anyhow!(e))?;

            if !running {
                info!("Checkpoint WindowTimer stop");
                // break;
            }
        }
    }
}

impl Runnable for SourceRunnable {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.context = Some(context.clone());

        self.task_id = context.task_descriptor.task_id;

        // first open next, then open self
        self.next_runnable.as_mut().unwrap().open(context)?;

        let input_split = context.task_descriptor.input_split.clone();
        let fun_context = context.to_fun_context(self.operator_id);
        let source_func = self.stream_source.operator_fn.as_mut();
        source_func.open(input_split, &fun_context)?;

        if let FunctionCreator::User = self.stream_source.fn_creator() {
            let stream_status_timer = context
                .window_timer
                .register("StreamStatus Event Timer", Duration::from_secs(10))
                .expect("register StreamStatus timer error");
            self.stream_status_timer = Some(stream_status_timer);

            let checkpoint_period = context.checkpoint_internal(Duration::from_secs(30));
            let checkpoint_timer = context
                .window_timer
                .register("Checkpoint Event Timer", checkpoint_period)
                .expect("register Checkpoint timer error");
            self.checkpoint_timer = Some(checkpoint_timer);
        }

        self.barrier_align = Some(BarrierAlign::new(context));

        info!(
            "SourceRunnable Opened, operator_id={:?}, task_id={:?}, BarrierAlign expected_size={:?}",
            self.operator_id, self.task_id, self.barrier_align.as_ref().unwrap().expected_size,
        );
        Ok(())
    }

    fn run(&mut self, mut _element: Element) {
        info!("{} running...", self.stream_source.operator_fn.name());

        let tags = vec![
            Tag::from(("job_id", self.task_id.job_id.0)),
            Tag::from(("task_number", self.task_id.task_number)),
        ];
        let metric_name = format!("Source_{}", self.stream_source.operator_fn.as_ref().name());
        let (sender, receiver) = named_channel(metric_name.as_str(), tags, 10240);
        let running = Arc::new(AtomicBool::new(true));

        self.poll_input_element(sender.clone(), running.clone());
        if let FunctionCreator::User = self.stream_source.fn_creator() {
            self.poll_stream_status(sender.clone(), running.clone());
            self.poll_checkpoint(sender.clone(), running.clone());
        }

        while let Ok(element) = receiver.recv() {
            if element.is_barrier() {
                let is_barrier_align = self
                    .barrier_align
                    .as_mut()
                    .unwrap()
                    .apply(element.as_barrier());
                if is_barrier_align {
                    debug!("barrier align and checkpoint");
                    let checkpoint_id = element.as_barrier().checkpoint_id;
                    let snapshot_context = {
                        let context = self.context.as_ref().unwrap();
                        context.checkpoint_context(self.operator_id, checkpoint_id)
                    };
                    self.checkpoint(snapshot_context);

                    self.next_runnable.as_mut().unwrap().run(element);
                }
            } else {
                self.next_runnable.as_mut().unwrap().run(element);
            }
        }
    }

    fn close(&mut self) -> anyhow::Result<()> {
        let source_func = self.stream_source.operator_fn.as_mut();
        source_func.close()?;

        // first close self, then close next
        self.next_runnable.as_mut().unwrap().close()
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext) {
        let handle = match self.stream_source.operator_fn.checkpoint_function() {
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

#[derive(Debug)]
struct BarrierAlign {
    expected_size: usize,

    checkpoint_id: u64,
    reached_size: usize,
}

impl BarrierAlign {
    pub fn new(context: &RunnableContext) -> Self {
        let parents = context
            .dag_metadata
            .job_parents(context.task_descriptor.task_id.job_id);
        let expected_size = parents
            .into_iter()
            .map(|(node, edge)| match edge {
                JobEdge::Forward => 1 as usize,
                JobEdge::ReBalance => node.parallelism as usize,
            })
            .sum();
        BarrierAlign {
            expected_size,
            checkpoint_id: 0,
            reached_size: 0,
        }
    }

    pub fn apply(&mut self, barrier: &Barrier) -> bool {
        if self.expected_size == 0 {
            return true;
        }

        let checkpoint_id = barrier.checkpoint_id.0;
        if self.checkpoint_id == checkpoint_id {
            self.reached_size += 1;

            if self.reached_size > self.expected_size {
                unreachable!()
            }

            self.reached_size == self.expected_size
        } else if self.checkpoint_id < checkpoint_id {
            self.checkpoint_id = checkpoint_id;
            self.reached_size = 1;

            self.reached_size == self.expected_size
        } else {
            error!(
                "barrier delay, current {}, reached {}",
                self.checkpoint_id, checkpoint_id
            );
            false
        }
    }
}
