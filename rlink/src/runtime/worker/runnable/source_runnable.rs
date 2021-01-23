use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

use crate::api::checkpoint::Checkpoint;
use crate::api::element::Element;
use crate::api::function::{InputFormat, InputSplit};
use crate::api::operator::{DefaultStreamOperator, FunctionCreator, TStreamOperator};
use crate::api::properties::SystemProperties;
use crate::api::runtime::{CheckpointId, OperatorId, TaskId};
use crate::channel::named_bounded;
use crate::channel::sender::ChannelSender;
use crate::metrics::{register_counter, Tag};
use crate::runtime::timer::TimerChannel;
use crate::runtime::worker::checkpoint::report_checkpoint;
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

    counter: Arc<AtomicU64>,
}

impl SourceRunnable {
    pub fn new(
        operator_id: OperatorId,
        input_split: InputSplit,
        stream_source: DefaultStreamOperator<dyn InputFormat>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create SourceRunnable input_split={:?}", &input_split);
        SourceRunnable {
            operator_id,
            context: None,
            task_id: TaskId::default(),

            stream_source,
            next_runnable,

            stream_status_timer: None,
            checkpoint_timer: None,

            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    fn poll_input_element(&mut self, sender: ChannelSender<Element>, running: Arc<AtomicBool>) {
        let iterator = self.stream_source.operator_fn.element_iter();
        std::thread::spawn(move || {
            for record in iterator {
                sender.send(record).unwrap();
            }

            running.store(false, Ordering::Relaxed);
        });
    }

    fn poll_stream_status(&mut self, sender: ChannelSender<Element>, running: Arc<AtomicBool>) {
        let stream_status_timer = self.stream_status_timer.as_ref().unwrap().clone();
        std::thread::spawn(move || loop {
            let running = running.load(Ordering::Relaxed);
            match stream_status_timer.recv() {
                Ok(window_time) => {
                    debug!("Trigger StreamStatus");
                    let stream_status = Element::new_stream_status(window_time, !running);

                    sender.send(stream_status).unwrap();
                }
                Err(_e) => {}
            }

            if !running {
                info!("StreamStatus WindowTimer stop");
                break;
            }
        });
    }

    fn poll_checkpoint(&mut self, sender: ChannelSender<Element>, running: Arc<AtomicBool>) {
        let checkpoint_timer = self.checkpoint_timer.as_ref().unwrap().clone();
        std::thread::spawn(move || loop {
            let running = running.load(Ordering::Relaxed);
            match checkpoint_timer.recv() {
                Ok(window_time) => {
                    debug!("Trigger Checkpoint");
                    let checkpoint_id = CheckpointId(window_time);
                    let barrier = Element::new_barrier(checkpoint_id);

                    sender.send(barrier).unwrap();
                }
                Err(_e) => {}
            }

            if !running {
                info!("Checkpoint WindowTimer stop");
                break;
            }
        });
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
            let checkpoint_period = context
                .application_descriptor
                .coordinator_manager
                .application_properties
                .get_checkpoint_internal()
                .unwrap_or(Duration::from_secs(30));

            let stream_status_timer = context
                .window_timer
                .register("StreamStatus Event Timer", Duration::from_secs(10))
                .expect("register StreamStatus timer error");
            self.stream_status_timer = Some(stream_status_timer);

            let checkpoint_timer = context
                .window_timer
                .register("Checkpoint Event Timer", checkpoint_period)
                .expect("register Checkpoint timer error");
            self.checkpoint_timer = Some(checkpoint_timer);
        }

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
            "Source_{}",
            self.stream_source.operator_fn.as_ref().get_name()
        );
        register_counter(metric_name.as_str(), tags, self.counter.clone());

        info!("Operator(SourceOperator) open");

        Ok(())
    }

    fn run(&mut self, mut _element: Element) {
        info!("{} running...", self.stream_source.operator_fn.get_name());

        let fn_creator = self.stream_source.get_fn_creator();

        let running = Arc::new(AtomicBool::new(true));
        let (sender, receiver) = named_bounded("", vec![], 1024);

        self.poll_input_element(sender.clone(), running.clone());

        if let FunctionCreator::User = fn_creator {
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

        let fn_name = self.stream_source.operator_fn.get_name();
        debug!("begin checkpoint : {}", fn_name);

        match self.stream_source.operator_fn.get_checkpoint() {
            Some(checkpoint) => {
                let ck_handle = checkpoint.snapshot_state(&context);
                let ck = Checkpoint {
                    operator_id: context.operator_id,
                    task_id: self.task_id,
                    checkpoint_id,
                    handle: ck_handle,
                };

                report_checkpoint(ck);
            }
            None => {}
        }
    }
}
