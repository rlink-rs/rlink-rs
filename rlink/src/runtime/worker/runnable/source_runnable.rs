use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::api::checkpoint::{Checkpoint, CheckpointHandle};
use crate::api::element::Element;
use crate::api::function::{InputFormat, InputSplit};
use crate::api::operator::{DefaultStreamOperator, FunctionCreator, TStreamOperator};
use crate::api::runtime::{CheckpointId, OperatorId, TaskId};
use crate::channel::named_channel;
use crate::channel::sender::ChannelSender;
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

        if let FunctionCreator::User = self.stream_source.get_fn_creator() {
            let stream_status_timer = context
                .window_timer
                .register("StreamStatus Event Timer", Duration::from_secs(10))
                .expect("register StreamStatus timer error");
            self.stream_status_timer = Some(stream_status_timer);

            let checkpoint_period = context.get_checkpoint_internal(Duration::from_secs(30));
            let checkpoint_timer = context
                .window_timer
                .register("Checkpoint Event Timer", checkpoint_period)
                .expect("register Checkpoint timer error");
            self.checkpoint_timer = Some(checkpoint_timer);
        }

        info!(
            "SourceRunnable Opened, operator_id={:?}, task_id={:?}",
            self.operator_id, self.task_id
        );
        Ok(())
    }

    fn run(&mut self, mut _element: Element) {
        info!("{} running...", self.stream_source.operator_fn.get_name());

        let tags = vec![
            Tag("job_id".to_string(), self.task_id.job_id.0.to_string()),
            Tag(
                "task_number".to_string(),
                self.task_id.task_number.to_string(),
            ),
        ];
        let metric_name = format!(
            "Source_{}",
            self.stream_source.operator_fn.as_ref().get_name()
        );
        let (sender, receiver) = named_channel(metric_name.as_str(), tags, 10240);
        let running = Arc::new(AtomicBool::new(true));

        self.poll_input_element(sender.clone(), running.clone());
        if let FunctionCreator::User = self.stream_source.get_fn_creator() {
            self.poll_stream_status(sender.clone(), running.clone());
            self.poll_checkpoint(sender.clone(), running.clone());
        }

        while let Ok(element) = receiver.recv() {
            if element.is_barrier() {
                let checkpoint_id = element.as_barrier().checkpoint_id;
                self.checkpoint(checkpoint_id);
            }

            self.next_runnable.as_mut().unwrap().run(element);
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

    fn checkpoint(&mut self, checkpoint_id: CheckpointId) {
        let context = {
            let context = self.context.as_ref().unwrap();
            context.get_checkpoint_context(self.operator_id, checkpoint_id)
        };

        let fn_name = self.stream_source.operator_fn.get_name().to_string();
        debug!("begin checkpoint : {}", fn_name);

        let ck = match self.stream_source.operator_fn.get_checkpoint() {
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
                "{} report checkpoint error. maybe report channel is full, checkpoint: {:?}",
                fn_name, ck
            )
        });
    }
}
