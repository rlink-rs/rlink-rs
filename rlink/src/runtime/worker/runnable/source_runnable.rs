use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::api::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::api::element::{Element, StreamStatus, Watermark};
use crate::api::function::InputFormat;
use crate::api::operator::{DefaultStreamOperator, FunctionCreator, TStreamOperator};
use crate::api::runtime::{CheckpointId, JobId, OperatorId, TaskId};
use crate::channel::named_channel;
use crate::channel::sender::ChannelSender;
use crate::channel::utils::iter::ChannelIterator;
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

    waiting_end_flags: usize,
    barrier_align: BarrierAlignManager,
    watermark_align: WatermarkAlignManagerV2,
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

            stream_status_timer: None,
            checkpoint_timer: None,

            waiting_end_flags: 0,
            barrier_align: BarrierAlignManager::default(),
            watermark_align: WatermarkAlignManagerV2::default(),
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
                break;
            }
        }
        Ok(())
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
            let window_time = checkpoint_timer.recv().map_err(|e| anyhow!(e))?;
            let barrier = Element::new_barrier(CheckpointId(window_time));
            sender.send(barrier).map_err(|e| anyhow!(e))?;

            let running = running.load(Ordering::Relaxed);
            if !running {
                info!("Checkpoint WindowTimer stop");
                break;
            }
        }
        Ok(())
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
            // job_parallelism.add(1);
        }

        // for (job_node, _job_edge) in context.parent_jobs() {
        //     parent_jobs.insert(job_node.job_id, job_node.parallelism);
        // }
        self.barrier_align = BarrierAlignManager::new(parent_execution_size);
        self.watermark_align = WatermarkAlignManagerV2::new(parent_jobs);

        info!(
            "SourceRunnable Opened, operator_id={:?}, task_id={:?}, ElementEventAlign parent_execution_size={:?}",
            self.operator_id, self.task_id, parent_execution_size,
        );
        Ok(())
    }

    fn run(&mut self, mut _element: Element) {
        info!("{} running...", self.stream_source.operator_fn.name());

        let mut element_iter = match self.stream_source.fn_creator() {
            FunctionCreator::User => {
                let tags = vec![
                    Tag::from(("job_id", self.task_id.job_id.0)),
                    Tag::from(("task_number", self.task_id.task_number)),
                ];
                let metric_name =
                    format!("Source_{}", self.stream_source.operator_fn.as_ref().name());
                let (sender, receiver) = named_channel(metric_name.as_str(), tags, 10240);
                let running = Arc::new(AtomicBool::new(true));

                self.poll_input_element(sender.clone(), running.clone());

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
                Element::Record(_) => self.next_runnable.as_mut().unwrap().run(element),
                Element::Barrier(barrier) => {
                    let is_barrier_align = self.barrier_align.apply(barrier.checkpoint_id.0);
                    if is_barrier_align {
                        debug!("barrier align and checkpoint");
                        let checkpoint_id = barrier.checkpoint_id;
                        let snapshot_context = {
                            let context = self.context.as_ref().unwrap();
                            context.checkpoint_context(self.operator_id, checkpoint_id)
                        };
                        self.checkpoint(snapshot_context);

                        self.next_runnable
                            .as_mut()
                            .unwrap()
                            .run(Element::Barrier(barrier));
                    }
                }
                Element::Watermark(watermark) => {
                    let align_watermark = self.watermark_align.apply_watermark(watermark);
                    match align_watermark {
                        Some(w) => {
                            info!(
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
                            let stream_status = Element::new_stream_status(0, true);
                            self.next_runnable.as_mut().unwrap().run(stream_status);

                            info!("break source loop");
                            break;
                        }
                    } else {
                        let (align, align_watermark) = self
                            .watermark_align
                            .apply_stream_status(stream_status.clone());
                        if align {
                            match align_watermark {
                                Some(w) => {
                                    info!(
                                        "Watermark&StreamStatus aligned, status_timestamp: {}",
                                        w.status_timestamp
                                    );
                                    self.next_runnable
                                        .as_mut()
                                        .unwrap()
                                        .run(Element::Watermark(w))
                                }
                                None => {
                                    info!(
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
struct AlignManager<T, E>
where
    T: Ord + PartialOrd + std::fmt::Display + Default,
{
    parent_execution_size: usize,

    batch_id: T,
    elements: Vec<E>,
}

impl<T, E> AlignManager<T, E>
where
    T: Ord + PartialOrd + std::fmt::Display + Default,
{
    pub fn new(parent_execution_size: usize, batch_id: T) -> Self {
        AlignManager {
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
enum StatusAlignManager {
    StreamStatus(AlignManager<u64, StreamStatus>),
    Watermark(AlignManager<u64, Watermark>),
}

impl StatusAlignManager {
    pub fn is_aligned(&self) -> bool {
        match self {
            StatusAlignManager::StreamStatus(stream_status_manager) => {
                stream_status_manager.is_aligned()
            }
            StatusAlignManager::Watermark(watermark_manager) => watermark_manager.is_aligned(),
        }
    }

    pub fn reset(&mut self, batch_id: u64) {
        match self {
            StatusAlignManager::StreamStatus(stream_status_manager) => {
                stream_status_manager.reset(batch_id);
            }
            StatusAlignManager::Watermark(watermark_manager) => {
                watermark_manager.reset(batch_id);
            }
        }
    }
}

#[derive(Debug, Default)]
struct WatermarkAlignManagerV2 {
    job_aligns: HashMap<JobId, StatusAlignManager>,
    parent_jobs: HashMap<JobId, u16>,

    current_status_timestamp: u64,
}

impl WatermarkAlignManagerV2 {
    pub fn new(parent_jobs: HashMap<JobId, u16>) -> Self {
        WatermarkAlignManagerV2 {
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
                StatusAlignManager::StreamStatus(_stream_status_manager) => None,
                StatusAlignManager::Watermark(watermark_manager) => watermark_manager
                    .elements()
                    .iter()
                    .filter(|w| !w.is_min())
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

            info!(
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
            StatusAlignManager::StreamStatus(AlignManager::new(
                parent_job_parallelism as usize,
                current_status_timestamp,
            ))
        });

        match align_manager {
            StatusAlignManager::StreamStatus(stream_status_manager) => {
                match stream_status_manager.apply(stream_status.timestamp, stream_status.clone()) {
                    Ok(_) => info!(
                        "status_timestamp: {}, apply StreamStatus from task {:?} success",
                        self.current_status_timestamp, stream_status.channel_key.source_task_id,
                    ),
                    Err(e) => error!(
                        "status_timestamp: {},apply StreamStatus from task {:?} error. {}",
                        self.current_status_timestamp, stream_status.channel_key.source_task_id, e
                    ),
                }
            }
            StatusAlignManager::Watermark(_watermark_manager) => {
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

            info!(
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
            StatusAlignManager::Watermark(AlignManager::new(
                parent_job_parallelism as usize,
                current_status_timestamp,
            ))
        });

        match align_manager {
            StatusAlignManager::StreamStatus(_stream_status_manager) => {
                error!("unreached, try apply a `StreamStatus` to WatermarkAlign");
                None
            }
            StatusAlignManager::Watermark(watermark_manager) => {
                match watermark_manager.apply(watermark.status_timestamp, watermark.clone()) {
                    Ok(_) => info!(
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
    use crate::runtime::worker::runnable::source_runnable::AlignManager;

    #[test]
    pub fn align_manager_test() {
        {
            let align_manager = AlignManager::<usize, usize>::new(0, 0);
            assert_eq!(align_manager.is_aligned(), true);
        }

        {
            let mut align_manager = AlignManager::new(1, 1);
            assert_eq!(align_manager.is_aligned(), false);

            align_manager.apply(1, 1).unwrap();
            assert_eq!(align_manager.is_aligned(), true);
        }

        {
            let mut align_manager = AlignManager::new(2, 1);
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

// #[derive(Debug, Default)]
// struct WatermarkAlign {
//     parent_execution_size: usize,
//
//     statue_timestamp: u64,
//     reached_size: usize,
//
//     watermarks: Vec<Watermark>,
// }
//
// impl WatermarkAlign {
//     pub fn new(parent_execution_size: usize, statue_timestamp: u64) -> Self {
//         WatermarkAlign {
//             parent_execution_size,
//             statue_timestamp,
//             reached_size: 0,
//             watermarks: Vec::new(),
//         }
//     }
//
//     fn min_watermark(&self) -> Option<Watermark> {
//         if self.parent_execution_size == 0 {
//             self.watermarks.get(0).map(|w| w.clone())
//         } else {
//             self.watermarks
//                 .iter()
//                 .min_by_key(|w| w.timestamp)
//                 .map(|w| w.clone())
//         }
//     }
//
//     pub fn apply_stream_status(
//         &mut self,
//         stream_status: &StreamStatus,
//     ) -> (bool, Option<Watermark>) {
//         self.apply(stream_status.timestamp, None)
//     }
//
//     pub fn apply_watermark(&mut self, watermark: Watermark) -> Option<Watermark> {
//         // ignore 0(`align`) field
//         self.apply(watermark.status_timestamp, Some(watermark)).1
//     }
//
//     fn apply(
//         &mut self,
//         status_timestamp: u64,
//         watermark: Option<Watermark>,
//     ) -> (bool, Option<Watermark>) {
//         if self.statue_timestamp != status_timestamp {
//             panic!("the `status_timestamp` confusion");
//         }
//
//         self.reached_size += 1;
//         if let Some(watermark) = watermark {
//             self.watermarks.push(watermark);
//         }
//
//         if self.reached_size > self.parent_execution_size {
//             unreachable!()
//         }
//
//         if self.reached_size == self.parent_execution_size {
//             (true, self.min_watermark())
//         } else {
//             (false, None)
//         }
//     }
// }
//
// #[derive(Debug, Default)]
// struct WatermarkAlignManager {
//     parent_execution_size: usize,
//     watermarks: BTreeMap<u64, WatermarkAlign>,
//     latest_align_status_timestamp: u64,
//     max_waiting_size: usize,
// }
//
// impl WatermarkAlignManager {
//     pub fn new(parent_execution_size: usize, max_waiting_size: usize) -> Self {
//         WatermarkAlignManager {
//             parent_execution_size,
//             watermarks: BTreeMap::new(),
//             latest_align_status_timestamp: 0,
//             max_waiting_size,
//         }
//     }
//
//     /// apply StreamStatus and align check
//     /// only align and return true
//     pub fn apply_stream_status(
//         &mut self,
//         stream_status: &StreamStatus,
//     ) -> (bool, Option<Watermark>) {
//         self.out_of_capacity_check();
//
//         let status_timestamp = stream_status.timestamp;
//         if status_timestamp <= self.latest_align_status_timestamp {
//             warn!("delay `StreamStatus` reached");
//             return (false, None);
//         }
//
//         let watermark_align =
//             self.watermarks
//                 .entry(status_timestamp)
//                 .or_insert(WatermarkAlign::new(
//                     self.parent_execution_size,
//                     status_timestamp,
//                 ));
//
//         let (align, watermark) = watermark_align.apply_stream_status(stream_status);
//         if align {
//             self.latest_align_status_timestamp = watermark_align.statue_timestamp;
//             let statue_timestamp = watermark_align.statue_timestamp;
//             self.watermarks.remove(&statue_timestamp);
//         } else {
//             if self.watermarks.len() > self.max_waiting_size {
//                 let status_timestamp = self
//                     .watermarks
//                     .iter()
//                     .next()
//                     .map(|(status_timestamp, _)| *status_timestamp)
//                     .unwrap();
//                 let watermark_align = self.watermarks.remove(&status_timestamp).unwrap();
//
//                 self.latest_align_status_timestamp = watermark_align.statue_timestamp;
//             }
//         }
//
//         (align, watermark)
//     }
//
//     /// apply Watermark and align check
//     /// align or expire return true
//     pub fn apply_watermark(&mut self, watermark: Watermark) -> Option<Watermark> {
//         self.out_of_capacity_check();
//
//         let status_timestamp = watermark.status_timestamp;
//         if status_timestamp <= self.latest_align_status_timestamp {
//             warn!("delay `StreamStatus` reached");
//             return None;
//         }
//
//         let watermark_align =
//             self.watermarks
//                 .entry(status_timestamp)
//                 .or_insert(WatermarkAlign::new(
//                     self.parent_execution_size,
//                     status_timestamp,
//                 ));
//
//         let mut align_watermark = watermark_align.apply_watermark(watermark);
//
//         if align_watermark.is_none() && self.watermarks.len() > self.max_waiting_size {
//             let status_timestamp = self
//                 .watermarks
//                 .iter()
//                 .next()
//                 .map(|(status_timestamp, _)| *status_timestamp)
//                 .unwrap();
//             let watermark_align = self.watermarks.remove(&status_timestamp).unwrap();
//             align_watermark = watermark_align.min_watermark()
//         }
//
//         match align_watermark {
//             Some(w) => {
//                 self.latest_align_status_timestamp = w.status_timestamp;
//                 self.watermarks.remove(&w.status_timestamp);
//                 Some(w)
//             }
//             None => None,
//         }
//     }
//
//     fn out_of_capacity_check(&mut self) {
//         if self.watermarks.len() <= 1 {
//             return;
//         }
//
//         let expired: Vec<u64> = self
//             .watermarks
//             .iter()
//             .map(|(status_timestamp, _)| *status_timestamp)
//             .filter(|status_timestamp| *status_timestamp <= self.latest_align_status_timestamp)
//             .collect();
//
//         for status_timestamp in expired {
//             self.watermarks.remove(&status_timestamp);
//             warn!("remove expire status: {}", status_timestamp);
//         }
//     }
// }
