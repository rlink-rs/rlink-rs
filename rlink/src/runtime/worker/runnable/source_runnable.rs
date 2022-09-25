use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;

use crate::channel::named_channel;
use crate::channel::sender::ChannelSender;
use crate::channel::utils::ChannelStream;
use crate::core::checkpoint::{Checkpoint, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::{Element, StreamStatus, Watermark};
use crate::core::function::{ElementStream, InputFormat, SendableElementStream};
use crate::core::operator::{DefaultStreamOperator, FunctionCreator, TStreamOperator};
use crate::core::runtime::{CheckpointId, JobId, OperatorId, TaskId};
use crate::core::watermark::MAX_WATERMARK;
use crate::metrics::metric::Counter;
use crate::metrics::register_counter;
use crate::runtime::timer::TimerChannel;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use crate::runtime::worker::WorkerTaskContext;
use crate::runtime::HeartbeatItem;

pub(crate) struct SourceRunnable {
    operator_id: OperatorId,
    context: Option<RunnableContext>,

    task_id: TaskId,
    daemon_task: bool,

    stream_source: DefaultStreamOperator<dyn InputFormat>,
    next_runnable: Option<Box<dyn Runnable>>,

    stream_status_timer: Option<TimerChannel>,
    checkpoint_timer: Option<TimerChannel>,

    waiting_end_flags: usize,
    barrier_alignment: AlignManager,
    stream_status_alignment: AlignManager,
    watermark_manager: WatermarkManager,

    counter: Counter,
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
            daemon_task: false,

            stream_source,
            next_runnable,

            stream_status_timer: None,
            checkpoint_timer: None,

            waiting_end_flags: 0,
            barrier_alignment: AlignManager::default(),
            stream_status_alignment: AlignManager::default(),
            watermark_manager: WatermarkManager::default(),
            counter: Counter::default(),
        }
    }

    async fn poll_input_element(
        &mut self,
        task_context: Arc<WorkerTaskContext>,
        sender: ChannelSender<Element>,
        running: Arc<AtomicBool>,
        daemon_task: bool,
    ) {
        let stream = self.stream_source.operator_fn.element_stream().await;

        tokio::spawn(async move {
            match SourceRunnable::poll_input_element0(
                task_context,
                stream,
                sender,
                running,
                daemon_task,
            )
            .await
            {
                Ok(_) => info!("poll input_element task finish"),
                Err(e) => panic!("poll_input_element thread error. {}", e),
            }
        });
    }

    async fn poll_input_element0(
        task_context: Arc<WorkerTaskContext>,
        mut stream: SendableElementStream,
        sender: ChannelSender<Element>,
        running: Arc<AtomicBool>,
        daemon_task: bool,
    ) -> anyhow::Result<()> {
        while let Some(element) = stream.next().await {
            sender.send(element).await.map_err(|e| anyhow!(e))?;

            if daemon_task && task_context.get_coordinator_status().is_terminating() {
                info!("daemon source stop by coordinator stop");
                break;
            }
        }

        running.store(false, Ordering::Relaxed);
        Ok(())
    }

    async fn poll_stream_status(
        &mut self,
        task_context: Arc<WorkerTaskContext>,
        sender: ChannelSender<Element>,
        running: Arc<AtomicBool>,
    ) {
        let stream_status_timer = self.stream_status_timer.take().unwrap();
        // let stream_status_timer = self.stream_status_timer.as_ref().unwrap().clone();
        tokio::spawn(async move {
            match SourceRunnable::poll_stream_status0(
                task_context,
                stream_status_timer,
                sender,
                running,
            )
            .await
            {
                Ok(_) => info!("poll stream_status task finish"),
                Err(e) => warn!("poll stream_status thread error. {}", e),
            }
        });
    }

    async fn poll_stream_status0(
        task_context: Arc<WorkerTaskContext>,
        mut stream_status_timer: TimerChannel,
        sender: ChannelSender<Element>,
        running: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        loop {
            let running = running.load(Ordering::Relaxed);

            let window_time = stream_status_timer
                .recv()
                .await
                .ok_or(anyhow!("timer closed"))?;
            let stream_status = Element::new_stream_status(window_time, !running);
            sender.send(stream_status).await.map_err(|e| anyhow!(e))?;

            if !running {
                info!("StreamStatus WindowTimer stop");
                if task_context.get_coordinator_status().is_terminated() {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn poll_checkpoint(
        &mut self,
        task_context: Arc<WorkerTaskContext>,
        sender: ChannelSender<Element>,
        running: Arc<AtomicBool>,
    ) {
        let checkpoint_timer = self.checkpoint_timer.take().unwrap();
        tokio::spawn(async move {
            match SourceRunnable::poll_checkpoint0(task_context, checkpoint_timer, sender, running)
                .await
            {
                Ok(_) => info!("poll checkpoint task finish"),
                Err(e) => warn!("poll checkpoint thread error. {}", e),
            }
        });
    }

    async fn poll_checkpoint0(
        task_context: Arc<WorkerTaskContext>,
        mut checkpoint_timer: TimerChannel,
        sender: ChannelSender<Element>,
        running: Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        loop {
            let window_time = checkpoint_timer
                .recv()
                .await
                .ok_or(anyhow!("timer closed"))?;
            let barrier = Element::new_barrier(CheckpointId(window_time));
            sender.send(barrier).await.map_err(|e| anyhow!(e))?;

            let running = running.load(Ordering::Relaxed);
            if !running {
                info!("Checkpoint WindowTimer stop");
                if task_context.get_coordinator_status().is_terminated() {
                    break;
                }
            }
        }
        Ok(())
    }

    fn task_context(&self) -> Arc<WorkerTaskContext> {
        self.context.as_ref().unwrap().task_context.clone()
    }

    async fn report_end_status(&self) {
        let heartbeat_publish = self.task_context().heartbeat_publish();
        heartbeat_publish
            .report(HeartbeatItem::TaskEnd {
                task_id: self.task_id,
            })
            .await;
    }
}

#[async_trait]
impl Runnable for SourceRunnable {
    async fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.context = Some(context.clone());

        self.task_id = context.task_context.task_descriptor.task_id;
        self.daemon_task = context.task_context.task_descriptor.daemon;

        // first open next, then open self
        self.next_runnable.as_mut().unwrap().open(context).await?;

        let input_split = context.task_context.task_descriptor.input_split.clone();
        let fun_context = context.to_fun_context(self.operator_id);
        let source_func = self.stream_source.operator_fn.as_mut();
        source_func.open(input_split, &fun_context).await?;

        if let FunctionCreator::User = self.stream_source.fn_creator() {
            let stream_status_timer = context
                .task_context
                .window_timer
                .register("StreamStatus Event Timer", Duration::from_secs(10))
                .expect("register StreamStatus timer error");
            self.stream_status_timer = Some(stream_status_timer);

            let checkpoint_period = context.checkpoint_interval(Duration::from_secs(30));
            let checkpoint_timer = context
                .task_context
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
        for (p_node, _edge) in context.parent_executions(&self.task_id) {
            let tasks = parent_jobs.entry(p_node.task_id.job_id).or_insert_with(|| {
                let tasks: Vec<bool> = (0..p_node.task_id.num_tasks)
                    .into_iter()
                    .map(|_| false)
                    .collect();
                tasks
            });

            tasks[p_node.task_id.task_number as usize] = true;
        }
        info!(
            "current job: {:?},parent jobs: {:?}",
            self.task_id, parent_jobs
        );

        self.barrier_alignment = AlignManager::new(parent_execution_size);
        self.stream_status_alignment = AlignManager::new(parent_execution_size);
        self.watermark_manager = WatermarkManager::new(parent_jobs);
        info!(
            "SourceRunnable Opened, operator_id={:?}, task_id={:?}, ElementEventAlign parent_execution_size={:?}",
            self.operator_id, self.task_id, parent_execution_size,
        );

        self.counter = register_counter(
            format!("Source_{}", self.stream_source.operator_fn.as_ref().name()),
            self.task_id.to_tags(),
        );

        Ok(())
    }

    async fn run(&mut self, mut _element: Element) {
        info!("{} running...", self.stream_source.operator_fn.name());

        let mut element_stream = match self.stream_source.fn_creator() {
            FunctionCreator::User => {
                let (sender, receiver) = named_channel(
                    format!("Source_{}", self.stream_source.operator_fn.as_ref().name()).as_str(),
                    self.task_id.to_tags(),
                    10240,
                );
                let running = Arc::new(AtomicBool::new(true));

                let task_context = self.context.as_ref().unwrap().task_context();
                self.poll_input_element(
                    task_context.clone(),
                    sender.clone(),
                    running.clone(),
                    self.daemon_task,
                )
                .await;

                self.poll_stream_status(task_context.clone(), sender.clone(), running.clone())
                    .await;
                self.poll_checkpoint(task_context.clone(), sender.clone(), running.clone())
                    .await;

                let stream: Pin<Box<dyn ElementStream + Send>> =
                    Box::pin(ChannelStream::new(receiver));
                stream
            }
            FunctionCreator::System => self.stream_source.operator_fn.element_stream().await,
        };

        let mut end_flags = 0;
        while let Some(element) = element_stream.next().await {
            match element {
                Element::Record(_) => {
                    self.next_runnable.as_mut().unwrap().run(element).await;
                    self.counter.fetch_add(1);
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
                        self.checkpoint(snapshot_context).await;

                        self.next_runnable
                            .as_mut()
                            .unwrap()
                            .run(Element::Barrier(barrier))
                            .await;
                    }
                }
                Element::Watermark(watermark) => match self.watermark_manager.apply(watermark) {
                    Some(min_watermark) => {
                        debug!(
                            "Watermark aligned, status_timestamp: {}",
                            min_watermark.timestamp
                        );
                        self.next_runnable
                            .as_mut()
                            .unwrap()
                            .run(Element::Watermark(min_watermark.clone()))
                            .await
                    }
                    None => {}
                },
                Element::StreamStatus(stream_status) => {
                    let parent_job_terminated = if stream_status.end {
                        end_flags += 1;
                        end_flags >= self.waiting_end_flags
                    } else {
                        false
                    };

                    self.watermark_manager.watermark_job_check(&stream_status);

                    let is_align = self.stream_status_alignment.apply(stream_status.timestamp);
                    if is_align {
                        debug!("stream_status align");
                        let stream_status = Element::new_stream_status(
                            stream_status.timestamp,
                            parent_job_terminated,
                        );
                        self.next_runnable
                            .as_mut()
                            .unwrap()
                            .run(stream_status)
                            .await;
                    }

                    if parent_job_terminated {
                        info!("all parents job stop on stream_status event");
                        self.report_end_status().await;
                    }
                }
            }
        }
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        let source_func = self.stream_source.operator_fn.as_mut();
        source_func.close().await?;

        // first close self, then close next
        self.next_runnable.as_mut().unwrap().close().await
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    async fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext) {
        let handle = self
            .stream_source
            .operator_fn
            .snapshot_state(&snapshot_context)
            .await
            .unwrap_or(CheckpointHandle::default());

        let ck = Checkpoint {
            operator_id: snapshot_context.operator_id,
            task_id: snapshot_context.task_id,
            checkpoint_id: snapshot_context.checkpoint_id,
            completed_checkpoint_id: snapshot_context.completed_checkpoint_id,
            handle,
        };
        snapshot_context.report(ck).map(|ck| {
            error!(
                "{:?} submit checkpoint error. maybe report channel is full, checkpoint: {:?}",
                snapshot_context.operator_id, ck
            )
        });
    }
}

#[derive(Debug, Default)]
struct AlignManager {
    parent_execution_size: usize,

    batch_id: u64,
    reached_size: usize,
}

impl AlignManager {
    pub fn new(parent_execution_size: usize) -> Self {
        AlignManager {
            parent_execution_size,
            batch_id: 0,
            reached_size: 0,
        }
    }

    /// apply a element event and check whether all event is reached.
    pub fn apply(&mut self, batch_id: u64) -> bool {
        if self.parent_execution_size == 0 {
            return true;
        }

        if self.batch_id == batch_id {
            self.reached_size += 1;

            if self.reached_size > self.parent_execution_size {
                unreachable!()
            }

            self.reached_size == self.parent_execution_size
        } else if self.batch_id < batch_id {
            self.batch_id = batch_id;
            self.reached_size = 1;

            self.reached_size == self.parent_execution_size
        } else {
            error!(
                "barrier delay, current {}, reached {}",
                self.batch_id, batch_id
            );
            false
        }
    }
}

#[derive(Debug)]
struct ParentWatermark {
    latest_watermark: Option<Watermark>,
    is_dependency: bool,
}

impl ParentWatermark {
    pub fn new(latest_watermark: Option<Watermark>, is_dependency: bool) -> Self {
        ParentWatermark {
            latest_watermark,
            is_dependency,
        }
    }

    pub fn update_watermark(&mut self, watermark: Watermark) {
        match &self.latest_watermark {
            Some(w) => {
                if watermark.timestamp > MAX_WATERMARK.timestamp {
                    // special `Watermark`, don't need to compare, just assign
                    self.latest_watermark = Some(watermark);
                } else if watermark.timestamp >= w.timestamp {
                    self.latest_watermark = Some(watermark);
                } else {
                    error!(
                        "low level watermark {:?} reached. current: {:?}",
                        watermark, self.latest_watermark
                    )
                }
            }
            None => self.latest_watermark = Some(watermark),
        }
    }
}

#[derive(Debug, Default)]
struct WatermarkManager {
    /// key: parent's JobId
    /// value: reached watermarks from parent job
    reached_watermarks: HashMap<JobId, Vec<ParentWatermark>>,
}

impl WatermarkManager {
    pub fn new(parent_jobs: HashMap<JobId, Vec<bool>>) -> Self {
        let mut reached_watermarks = HashMap::new();
        for (job_id, tasks) in parent_jobs {
            let watermarks: Vec<ParentWatermark> = tasks
                .into_iter()
                .map(|x| ParentWatermark::new(None, x))
                .collect();

            reached_watermarks.insert(job_id, watermarks);
        }

        WatermarkManager { reached_watermarks }
    }

    pub fn apply(&mut self, watermark: Watermark) -> Option<Watermark> {
        // no parent jobs
        if self.reached_watermarks.is_empty() {
            return Some(watermark);
        }

        let source_task_id = &watermark.channel_key.source_task_id;
        match self.reached_watermarks.get_mut(&source_task_id.job_id) {
            Some(watermarks) => {
                let task_number = source_task_id.task_number as usize;
                if task_number >= watermarks.len() {
                    panic!(
                        "unreached! parent job's parallelism is {}, but reached `task_number` {}",
                        watermarks.len(),
                        task_number
                    );
                }
                if !watermarks[task_number].is_dependency {
                    panic!("Does not depend on the task, {:?}", watermark.channel_key);
                }

                watermarks[task_number].update_watermark(watermark);
            }
            None => panic!("unreached! the JobId of `Watermark` is not in the parent jobs list, or the job key has removed"),
        }

        self.min_watermark()
    }

    /// find the min watermark
    pub fn min_watermark(&self) -> Option<Watermark> {
        let mut min_watermark: Option<&Watermark> = None;
        for (_job_id, watermarks) in &self.reached_watermarks {
            for p_watermark in watermarks {
                // skip placeholder `Watermark`
                if !p_watermark.is_dependency {
                    continue;
                }

                match &p_watermark.latest_watermark {
                    Some(watermark) => {
                        if let Some(w) = min_watermark {
                            if watermark.timestamp < w.timestamp {
                                min_watermark = Some(watermark);
                            }
                        } else {
                            min_watermark = Some(watermark);
                        }
                    }
                    None => {
                        return None;
                    }
                }
            }
        }

        min_watermark.map(|w| w.clone())
    }

    pub fn watermark_job_check(&mut self, stream_status: &StreamStatus) {
        let source_task_id = &stream_status.channel_key.source_task_id;
        let non_watermark_job = match self.reached_watermarks.get(&source_task_id.job_id) {
            Some(watermarks) => {
                let task_number = source_task_id.task_number as usize;
                if task_number >= watermarks.len() {
                    panic!(
                        "unreached! parent job's parallelism is {}, but reached `task_number` {}",
                        watermarks.len(),
                        task_number
                    );
                }
                watermarks[task_number].latest_watermark.is_none()
            }
            None => false,
        };

        if non_watermark_job {
            self.reached_watermarks.remove(&source_task_id.job_id);
            info!(
                "remove non watermark job_id={:?} from WatermarkManager",
                source_task_id.job_id
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::core::element::{StreamStatus, Watermark};
    use crate::core::runtime::{ChannelKey, JobId, TaskId};
    use crate::runtime::worker::runnable::source_runnable::WatermarkManager;

    fn gen_watermark(timestamp: u64, job_id: u32, task_number: u16, num_tasks: u16) -> Watermark {
        let mut watermark = Watermark::new(timestamp);
        watermark.channel_key = ChannelKey {
            source_task_id: TaskId {
                job_id: JobId(job_id),
                task_number,
                num_tasks,
            },
            target_task_id: Default::default(),
        };

        watermark
    }

    fn gen_stream_status(
        timestamp: u64,
        job_id: u32,
        task_number: u16,
        num_tasks: u16,
    ) -> StreamStatus {
        let mut stream_status = StreamStatus::new(timestamp, false);
        stream_status.channel_key = ChannelKey {
            source_task_id: TaskId {
                job_id: JobId(job_id),
                task_number,
                num_tasks,
            },
            target_task_id: Default::default(),
        };

        stream_status
    }

    #[test]
    pub fn watermark_manager_test() {
        let mut parent_jobs = HashMap::new();
        parent_jobs.insert(JobId(1), vec![true, true]);
        parent_jobs.insert(JobId(2), vec![true, true, true]);

        let mut watermark_manager = WatermarkManager::new(parent_jobs);

        {
            let watermark = gen_watermark(10, 1, 0, 2);
            let w = watermark_manager.apply(watermark);
            assert_eq!(w, None);
        }

        {
            let stream_status = gen_stream_status(20, 2, 0, 3);
            watermark_manager.watermark_job_check(&stream_status);
        }

        {
            let watermark = gen_watermark(9, 1, 1, 2);
            let w = watermark_manager.apply(watermark);
            assert_eq!(w.unwrap().timestamp, 9);
        }
    }
}
