use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::api::checkpoint::Checkpoint;
use crate::api::element::Element;
use crate::api::function::{InputFormat, InputSplit};
use crate::api::operator::{FunctionCreator, StreamOperator, TStreamOperator};
use crate::api::properties::SystemProperties;
use crate::metrics::{register_counter, Tag};
use crate::runtime::worker::checkpoint::report_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use crate::utils::timer::TimerChannel;

#[derive(Debug)]
pub(crate) struct SourceRunnable {
    context: Option<RunnableContext>,

    job_id: u32,
    task_number: u16,

    stream_source: StreamOperator<dyn InputFormat>,
    next_runnable: Option<Box<dyn Runnable>>,

    stream_status_timer: Option<TimerChannel>,
    checkpoint_timer: Option<TimerChannel>,

    counter: Arc<AtomicU64>,
}

impl SourceRunnable {
    pub fn new(
        input_split: InputSplit,
        stream_source: StreamOperator<dyn InputFormat>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create SourceRunnable input_split={:?}", &input_split);
        SourceRunnable {
            context: None,
            job_id: 0,
            task_number: 0,

            stream_source,
            next_runnable,

            stream_status_timer: None,
            checkpoint_timer: None,

            counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Runnable for SourceRunnable {
    fn open(&mut self, context: &RunnableContext) {
        self.context = Some(context.clone());

        self.job_id = context.task_descriptor.task_id.job_id;
        self.task_number = context.task_descriptor.task_id.task_number;

        // first open next, then open self
        self.next_runnable.as_mut().unwrap().open(context);

        let input_split = context.task_descriptor.input_split.clone();

        let fun_context = context.to_fun_context();
        let source_func = self.stream_source.operator_fn.as_mut();
        source_func.open(input_split, &fun_context);

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
                context.task_descriptor.task_id.job_id.to_string(),
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
    }

    fn run(&mut self, mut _element: Element) {
        info!("{} running...", self.stream_source.operator_fn.get_name());

        let fn_creator = self.stream_source.get_fn_creator();

        let mut idle_counter = 0u32;
        let idle_delay_10 = Duration::from_millis(10);
        let idle_delay_300 = Duration::from_millis(300);

        loop {
            let mut end = false;
            let mut counter = 0;
            for _ in 0..1000 {
                end = self.stream_source.operator_fn.as_mut().reached_end();
                if end {
                    break;
                }

                match self.stream_source.operator_fn.as_mut().next_element() {
                    Some(row) => {
                        if row.is_watermark() {
                            debug!(
                                "Source `next_element` get Watermark({})",
                                row.as_watermark().timestamp
                            );
                        } else if row.is_barrier() {
                            // if `InputFormat` return the Barrier, fire checkpoint immediately
                            self.checkpoint(row.as_barrier().checkpoint_id);
                        }

                        self.next_runnable.as_mut().unwrap().run(row);
                        counter += 1;
                    }
                    None => break,
                }
            }

            if counter > 0 {
                self.counter.fetch_add(counter as u64, Ordering::Relaxed);
            }

            if let FunctionCreator::User = fn_creator {
                if let Ok(window_time) = self.stream_status_timer.as_ref().unwrap().try_recv() {
                    debug!("Trigger StreamStatus");
                    let stream_status = Element::new_stream_status(window_time, end);
                    self.next_runnable.as_mut().unwrap().run(stream_status);
                };

                if let Ok(window_time) = self.checkpoint_timer.as_ref().unwrap().try_recv() {
                    debug!("Trigger Checkpoint");
                    let barrier = Element::new_barrier(window_time);

                    self.checkpoint(window_time);

                    self.next_runnable.as_mut().unwrap().run(barrier);
                };
            }

            // if end {
            //     info!("source end");
            //     std::thread::sleep(Duration::from_secs(3600));
            //     // break;
            // }

            if counter == 0 {
                idle_counter += 1;

                // idle timeout = sleep(10millis) * 100 * 60 = 1min
                if idle_counter > 100 * 60 {
                    idle_counter = 0;
                    info!("{} idle", self.stream_source.operator_fn.get_name());
                }

                // empty loop tolerate
                if idle_counter < 30 {
                    std::thread::sleep(idle_delay_10);
                } else {
                    std::thread::sleep(idle_delay_300);
                }
            } else {
                idle_counter = 0;
            }
        }
    }

    fn close(&mut self) {
        let source_func = self.stream_source.operator_fn.as_mut();
        source_func.close();

        // first close self, then close next
        self.next_runnable.as_mut().unwrap().close();
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, checkpoint_id: u64) {
        let context = {
            let context = self.context.as_ref().unwrap();
            context.get_checkpoint_context(checkpoint_id)
        };

        let fn_name = self.stream_source.operator_fn.get_name();
        debug!("begin checkpoint : {}", fn_name);

        match self.stream_source.operator_fn.get_checkpoint() {
            Some(checkpoint) => {
                let ck_handle = checkpoint.snapshot_state(&context);
                let ck = Checkpoint {
                    job_id: self.job_id,
                    task_num: self.task_number,
                    checkpoint_id,
                    handle: ck_handle,
                };

                report_checkpoint(ck);
            }
            None => {}
        }
    }
}
