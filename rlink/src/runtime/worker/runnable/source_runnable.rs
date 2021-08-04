use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

use crate::channel::named_channel;
use crate::channel::sender::ChannelSender;
use crate::channel::utils::iter::ChannelIterator;
use crate::core::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::{Element, StreamStatus, Watermark};
use crate::core::function::InputFormat;
use crate::core::operator::{DefaultStreamOperator, FunctionCreator, TStreamOperator};
use crate::core::runtime::{CheckpointId, JobId, OperatorId, TaskId};
use crate::runtime::timer::TimerChannel;
use crate::runtime::worker::backpressure::Backpressure;
use crate::runtime::worker::checkpoint::submit_checkpoint;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

#[derive(Debug)]
pub(crate) struct SourceRunnable {
    operator_id: OperatorId,
    context: Option<RunnableContext>,

    task_id: TaskId,

    stream_source: DefaultStreamOperator<dyn InputFormat>,
    next_runnable: Option<Box<dyn Runnable>>,

    backpressure: Option<Backpressure>,

    stream_status_timer: Option<TimerChannel>,
    checkpoint_timer: Option<TimerChannel>,

    waiting_end_flags: usize,
    barrier_alignment: BarrierAlignManager,
    status_alignment: StatusAlignmentManager,
}

impl SourceRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_source: DefaultStreamOperator<dyn InputFormat>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        SourceRunnable {
            operator_id,
            context: None,
            task_id: TaskId::default(),

            stream_source,
            next_runnable,

            backpressure: None,

            stream_status_timer: None,
            checkpoint_timer: None,

            waiting_end_flags: 0,
            barrier_alignment: BarrierAlignManager::default(),
            status_alignment: StatusAlignmentManager::default(),
        }
    }

    fn poll_input_element(
        &mut self,
        sender: ChannelSender<Element>,
        running: Arc<AtomicBool>,
        bp_pause_dur: Arc<AtomicU64>,
    ) {
        let iterator = self.stream_source.operator_fn.element_iter();
        crate::utils::thread::spawn("poll_input_element", move || {
            match SourceRunnable::poll_input_element0(iterator, sender, running, bp_pause_dur) {
                Ok(_) => {}
                Err(e) => panic!("poll_input_element thread error. {}", e),
            }
        });
    }

    fn poll_input_element0(
        iterator: Box<dyn Iterator<Item = Element> + Send>,
        sender: ChannelSender<Element>,
        running: Arc<AtomicBool>,
        bp_pause_dur: Arc<AtomicU64>,
    ) -> anyhow::Result<()> {
        for record in iterator {
            let v = bp_pause_dur.load(Ordering::Relaxed);
            if v > 0 {
                bp_pause_dur.fetch_sub(v, Ordering::Relaxed);

                info!("backpressure for {}ms", v);
                std::thread::sleep(Duration::from_millis(v));
            }

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
                Err(e) => warn!("poll stream_status thread error. {}", e),
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
        // Ok(())
    }

    fn poll_checkpoint(&mut self, sender: ChannelSender<Element>, running: Arc<AtomicBool>) {
        let checkpoint_timer = self.checkpoint_timer.as_ref().unwrap().clone();
        crate::utils::thread::spawn("poll_checkpoint", move || {
            match SourceRunnable::poll_checkpoint0(checkpoint_timer, sender, running) {
                Ok(_) => {}
                Err(e) => warn!("poll checkpoint thread error. {}", e),
            }
        });
    }

    fn poll_checkpoint0(
        checkpoint_timer: TimerChannel,
        sender: ChannelSender<Element>,
        running: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        loop {
            let window_time = checkpoint_timer.recv().map_err(|e| anyhow!(e))?;
            let barrier = Element::new_barrier(CheckpointId(window_time));
            sender.send(barrier).map_err(|e| anyhow!(e))?;

            let running = running.load(Ordering::Relaxed);
            if !running {
                info!("Checkpoint WindowTimer stop");
                // break;
            }
        }
        // Ok(())
    }
}

impl Runnable for SourceRunnable {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.context = Some(context.clone());

        self.task_id = context.task_descriptor.task_id;

        // first open next, then open self
        self.next_runnable.as_mut().unwrap().open(context)?;

        self.backpressure = Some(context.backpressure.clone());

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

            let checkpoint_period = context.checkpoint_interval(Duration::from_secs(30));
            let checkpoint_timer = context
                .window_timer
                .register("Checkpoint Event Timer", checkpoint_period)
                .expect("register Checkpoint timer error");
            self.checkpoint_timer = Some(checkpoint_timer);
        }

        let parent_execution_size = context.parent_executions(&self.task_id).len();
        self.waiting_end_flags = if parent_execution_size == 0 {
            1
        } else {
            parent_execution_size
        };

        let mut parent_jobs = HashMap::new();
        for (node, _edge) in context.parent_executions(&self.task_id) {
            let job_parallelism = parent_jobs.entry(node.task_id.job_id).or_insert(0);
            *job_parallelism = *job_parallelism + 1;
        }

        self.barrier_alignment = BarrierAlignManager::new(parent_execution_size);
        self.status_alignment = StatusAlignmentManager::new(parent_jobs);

        info!(
            "SourceRunnable Opened, operator_id={:?}, task_id={:?}, ElementEventAlign parent_execution_size={:?}",
            self.operator_id, self.task_id, parent_execution_size,
        );
        Ok(())
    }

    fn run(&mut self, mut _element: Element) {
        info!("{} running...", self.stream_source.operator_fn.name());

        let bp_pause_dur = Arc::new(AtomicU64::new(0));

        let mut element_iter = match self.stream_source.fn_creator() {
            FunctionCreator::User => {
                let (sender, receiver) = named_channel(
                    format!("Source_{}", self.stream_source.operator_fn.as_ref().name()).as_str(),
                    self.task_id.to_tags(),
                    10240,
                );
                let running = Arc::new(AtomicBool::new(true));

                self.poll_input_element(sender.clone(), running.clone(), bp_pause_dur.clone());

                self.poll_stream_status(sender.clone(), running.clone());
                self.poll_checkpoint(sender.clone(), running.clone());

                let element_iter: Box<dyn Iterator<Item = Element> + Send> =
                    Box::new(ChannelIterator::new(receiver));
                element_iter
            }
            FunctionCreator::System => self.stream_source.operator_fn.element_iter(),
        };

        let mut end_flags = 0;
        while let Some(element) = element_iter.next() {
            match element {
                Element::Record(_) => {
                    self.next_runnable.as_mut().unwrap().run(element);
                    if let Some(v) = self.backpressure.as_ref().unwrap().take() {
                        bp_pause_dur.fetch_add(v.as_millis() as u64, Ordering::Relaxed);
                    }
                }
                Element::Barrier(barrier) => {
                    let is_barrier_align = self.barrier_alignment.apply(barrier.checkpoint_id.0);
                    if is_barrier_align {
                        debug!("barrier align and checkpoint");
                        let checkpoint_id = barrier.checkpoint_id;
                        let snapshot_context = {
                            let context = self.context.as_ref().unwrap();
                            context.checkpoint_context(self.operator_id, checkpoint_id, None)
                        };
                        self.checkpoint(snapshot_context);

                        self.next_runnable
                            .as_mut()
                            .unwrap()
                            .run(Element::Barrier(barrier));
                    }
                }
                Element::Watermark(watermark) => {
                    if watermark.end() {
                        end_flags += 1;
                        if end_flags == self.waiting_end_flags {
                            info!("all parents job stop on watermark event");
                        }
                        // todo mark the job as end status
                    }

                    let align_watermark = self.status_alignment.apply_watermark(watermark);
                    match align_watermark {
                        Some(w) => {
                            debug!(
                                "Watermark aligned, status_timestamp: {}",
                                w.status_timestamp
                            );
                            self.next_runnable
                                .as_mut()
                                .unwrap()
                                .run(Element::Watermark(w))
                        }
                        None => {}
                    }
                }
                Element::StreamStatus(stream_status) => {
                    if stream_status.end {
                        end_flags += 1;
                        if end_flags == self.waiting_end_flags {
                            // todo mark the job as end status
                            info!("all parents job stop on stream_status event");
                        }
                    }
                    let (align, align_watermark) = self
                        .status_alignment
                        .apply_stream_status(stream_status.clone());
                    if align {
                        match align_watermark {
                            Some(w) => {
                                debug!(
                                    "Watermark&StreamStatus aligned, status_timestamp: {}",
                                    w.status_timestamp
                                );
                                self.next_runnable
                                    .as_mut()
                                    .unwrap()
                                    .run(Element::Watermark(w))
                            }
                            None => {
                                debug!(
                                    "StreamStatus aligned, status_timestamp: {}",
                                    stream_status.timestamp
                                );
                                self.next_runnable
                                    .as_mut()
                                    .unwrap()
                                    .run(Element::StreamStatus(stream_status))
                            }
                        }
                    }
                }
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
        let handle = self
            .stream_source
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

#[derive(Debug, Default)]
struct BarrierAlignManager {
    parent_execution_size: usize,

    checkpoint_id: u64,
    reached_size: usize,
}

impl BarrierAlignManager {
    pub fn new(parent_execution_size: usize) -> Self {
        BarrierAlignManager {
            parent_execution_size,
            checkpoint_id: 0,
            reached_size: 0,
        }
    }

    pub fn apply(&mut self, checkpoint_id: u64) -> bool {
        if self.parent_execution_size == 0 {
            return true;
        }

        if self.checkpoint_id == checkpoint_id {
            self.reached_size += 1;

            if self.reached_size > self.parent_execution_size {
                unreachable!()
            }

            self.reached_size == self.parent_execution_size
        } else if self.checkpoint_id < checkpoint_id {
            self.checkpoint_id = checkpoint_id;
            self.reached_size = 1;

            self.reached_size == self.parent_execution_size
        } else {
            error!(
                "barrier delay, current {}, reached {}",
                self.checkpoint_id, checkpoint_id
            );
            false
        }
    }
}

#[derive(Debug, Default)]
struct Alignment<T, E>
where
    T: Ord + PartialOrd + std::fmt::Display + Default,
{
    parent_execution_size: usize,

    batch_id: T,
    elements: Vec<E>,
}

impl<T, E> Alignment<T, E>
where
    T: Ord + PartialOrd + std::fmt::Display + Default,
{
    pub fn new(parent_execution_size: usize, batch_id: T) -> Self {
        Alignment {
            parent_execution_size,
            batch_id,
            elements: Vec::new(),
        }
    }

    pub fn elements(&self) -> &[E] {
        self.elements.as_slice()
    }

    pub fn reset(&mut self, batch_id: T) {
        self.batch_id = batch_id;
        self.elements.clear();
    }

    pub fn is_aligned(&self) -> bool {
        self.elements.len() == self.parent_execution_size
    }

    pub fn apply(&mut self, batch_id: T, element: E) -> anyhow::Result<()> {
        if self.parent_execution_size == 0 {
            return Ok(());
        }

        match self.batch_id.cmp(&batch_id) {
            std::cmp::Ordering::Equal => {
                self.elements.push(element);

                if self.elements.len() > self.parent_execution_size {
                    return Err(anyhow!(
                        "reached elements more than expect size {}",
                        self.parent_execution_size
                    ));
                }
            }
            std::cmp::Ordering::Less => {
                self.batch_id = batch_id;
                self.elements.push(element);
            }
            std::cmp::Ordering::Greater => {
                return Err(anyhow!(
                    "event delay, current {}, reached {}",
                    self.batch_id,
                    batch_id
                ));
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
enum StatusAlignment {
    StreamStatus(Alignment<u64, StreamStatus>),
    Watermark(Alignment<u64, Watermark>),
}

impl StatusAlignment {
    pub fn is_aligned(&self) -> bool {
        match self {
            StatusAlignment::StreamStatus(stream_status_manager) => {
                stream_status_manager.is_aligned()
            }
            StatusAlignment::Watermark(watermark_manager) => watermark_manager.is_aligned(),
        }
    }

    pub fn reset(&mut self, batch_id: u64) {
        match self {
            StatusAlignment::StreamStatus(stream_status_manager) => {
                stream_status_manager.reset(batch_id);
            }
            StatusAlignment::Watermark(watermark_manager) => {
                watermark_manager.reset(batch_id);
            }
        }
    }
}

#[derive(Debug, Default)]
struct StatusAlignmentManager {
    job_aligns: HashMap<JobId, StatusAlignment>,
    parent_jobs: HashMap<JobId, u16>,

    current_status_timestamp: u64,
}

impl StatusAlignmentManager {
    pub fn new(parent_jobs: HashMap<JobId, u16>) -> Self {
        StatusAlignmentManager {
            job_aligns: HashMap::new(),
            parent_jobs,
            current_status_timestamp: 0,
        }
    }

    fn clean(&mut self, status_timestamp: u64) {
        for (_job_id, align_manager) in self.job_aligns.borrow_mut() {
            align_manager.reset(status_timestamp);
        }
    }

    fn is_aligned(&self) -> bool {
        let aligned_count = self
            .parent_jobs
            .iter()
            .filter(|(job_id, _)| {
                self.job_aligns
                    .get(&job_id)
                    .map(|align_manager| align_manager.is_aligned())
                    .unwrap_or(false)
            })
            .count();

        self.parent_jobs.len() == aligned_count
    }

    fn min_watermark(&self) -> Option<Watermark> {
        self.job_aligns
            .iter()
            .filter_map(|(_job_id, align_manager)| match align_manager {
                StatusAlignment::StreamStatus(_stream_status_manager) => None,
                StatusAlignment::Watermark(watermark_manager) => watermark_manager
                    .elements()
                    .iter()
                    // .filter(|w| !w.is_min())
                    .min_by_key(|w| w.timestamp),
            })
            .min_by_key(|w| w.timestamp)
            .map(|w| w.clone())
    }

    fn apply_stream_status(&mut self, stream_status: StreamStatus) -> (bool, Option<Watermark>) {
        if self.parent_jobs.len() == 0 {
            return (true, None);
        }

        if stream_status.timestamp < self.current_status_timestamp {
            error!(
                "delay StreamStatus reached. {} < {}",
                stream_status.timestamp, self.current_status_timestamp
            );
            return (false, None);
        }

        if stream_status.timestamp > self.current_status_timestamp {
            if self.current_status_timestamp > 0 && !self.is_aligned() {
                error!("the new StreamStatus reached");
            }

            debug!(
                "new status reached. status_timestamp: {}, old: {}",
                stream_status.timestamp, self.current_status_timestamp,
            );
            self.current_status_timestamp = stream_status.timestamp;
            self.clean(self.current_status_timestamp);
        }

        let parent_job_id = stream_status.channel_key.source_task_id.job_id;
        let parent_job_parallelism = match self.parent_jobs.get(&parent_job_id) {
            Some(parent_job_parallelism) => *parent_job_parallelism,
            None => panic!("parent job not found. {:?}", parent_job_id),
        };
        let current_status_timestamp = self.current_status_timestamp;
        let align_manager = self.job_aligns.entry(parent_job_id).or_insert_with(|| {
            StatusAlignment::StreamStatus(Alignment::new(
                parent_job_parallelism as usize,
                current_status_timestamp,
            ))
        });

        match align_manager {
            StatusAlignment::StreamStatus(stream_status_manager) => {
                match stream_status_manager.apply(stream_status.timestamp, stream_status.clone()) {
                    Ok(_) => debug!(
                        "status_timestamp: {}, apply StreamStatus from task {:?} success",
                        self.current_status_timestamp, stream_status.channel_key.source_task_id,
                    ),
                    Err(e) => error!(
                        "status_timestamp: {},apply StreamStatus from task {:?} error. {}",
                        self.current_status_timestamp, stream_status.channel_key.source_task_id, e
                    ),
                }
            }
            StatusAlignment::Watermark(_watermark_manager) => {
                error!("unreached, try apply a `Watermark` to StreamStatusAlign");
            }
        }

        if self.is_aligned() {
            (true, self.min_watermark())
        } else {
            (false, None)
        }
    }

    fn apply_watermark(&mut self, watermark: Watermark) -> Option<Watermark> {
        if self.parent_jobs.len() == 0 {
            return Some(watermark);
        }

        if watermark.status_timestamp < self.current_status_timestamp {
            error!("delay Watermark reached. {}", watermark.status_timestamp);
            return None;
        }

        if watermark.status_timestamp > self.current_status_timestamp {
            if self.current_status_timestamp > 0 && !self.is_aligned() {
                error!("the new Watermark reached before un-align");
            }

            debug!(
                "new Watermark reached. status_timestamp: {}, old: {}",
                watermark.status_timestamp, self.current_status_timestamp,
            );
            self.current_status_timestamp = watermark.status_timestamp;
            self.clean(self.current_status_timestamp);
        }

        let parent_job_id = watermark.channel_key.source_task_id.job_id;
        let parent_job_parallelism = *self.parent_jobs.get(&parent_job_id).unwrap();
        let current_status_timestamp = self.current_status_timestamp;
        let align_manager = self.job_aligns.entry(parent_job_id).or_insert_with(|| {
            StatusAlignment::Watermark(Alignment::new(
                parent_job_parallelism as usize,
                current_status_timestamp,
            ))
        });

        match align_manager {
            StatusAlignment::StreamStatus(_stream_status_manager) => {
                error!("unreached, try apply a `StreamStatus` to WatermarkAlign");
                None
            }
            StatusAlignment::Watermark(watermark_manager) => {
                match watermark_manager.apply(watermark.status_timestamp, watermark.clone()) {
                    Ok(_) => debug!(
                        "status_timestamp: {}, apply Watermark from task: {:?} success",
                        self.current_status_timestamp, watermark.channel_key.source_task_id,
                    ),
                    Err(e) => error!(
                        "status_timestamp: {}, apply Watermark from task: {:?} error. {}",
                        self.current_status_timestamp, watermark.channel_key.source_task_id, e
                    ),
                }

                if self.is_aligned() {
                    self.min_watermark()
                } else {
                    None
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::worker::runnable::source_runnable::Alignment;

    #[test]
    pub fn align_manager_test() {
        {
            let align_manager = Alignment::<usize, usize>::new(0, 0);
            assert_eq!(align_manager.is_aligned(), true);
        }

        {
            let mut align_manager = Alignment::new(1, 1);
            assert_eq!(align_manager.is_aligned(), false);

            align_manager.apply(1, 1).unwrap();
            assert_eq!(align_manager.is_aligned(), true);
        }

        {
            let mut align_manager = Alignment::new(2, 1);
            assert_eq!(align_manager.is_aligned(), false);

            align_manager.apply(1, 1).unwrap();
            assert_eq!(align_manager.is_aligned(), false);

            align_manager.apply(1, 2).unwrap();
            assert_eq!(align_manager.is_aligned(), true);

            assert_eq!(align_manager.apply(1, 3).is_err(), true);
            assert_eq!(align_manager.apply(0, 4).is_err(), true);

            align_manager.reset(2);

            align_manager.apply(2, 0).unwrap();
            assert_eq!(align_manager.is_aligned(), false);

            align_manager.apply(2, 1).unwrap();
            assert_eq!(align_manager.is_aligned(), true);
        }
    }
}
