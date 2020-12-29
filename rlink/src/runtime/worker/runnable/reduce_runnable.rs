use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::api::backend::KeyedStateBackend;
use crate::api::element::{Barrier, Element, Record, Watermark};
use crate::api::function::{KeySelectorFunction, ReduceFunction};
use crate::api::operator::StreamOperator;
use crate::api::properties::SystemProperties;
use crate::api::window::{Window, WindowWrap};
use crate::metrics::{register_counter, Tag};
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use crate::storage::keyed_state::{WindowState, WindowStateWrap};
use crate::utils::date_time::timestamp_str;

#[derive(Debug)]
pub(crate) struct ReduceRunnable {
    task_id: Option<String>,
    task_number: u16,
    dependency_parallelism: u32,

    stream_key_by: Option<StreamOperator<dyn KeySelectorFunction>>,
    stream_reduce: StreamOperator<dyn ReduceFunction>,
    next_runnable: Option<Box<dyn Runnable>>,

    state: Option<WindowStateWrap>, // HashMap<Vec<u8>, Record>, // HashMap<TimeWindow, HashMap<Record, Record>>,

    current_checkpoint_id: u64,
    reached_barriers: Vec<Barrier>,

    max_watermark_status_timestamp: u64,
    watermark_align: Option<WatermarkAlign>,
    // the Record can be operate after this window(include this window's time)
    limited_watermark_window: Option<WindowWrap>,

    counter: Arc<AtomicU64>,
    expire_counter: Arc<AtomicU64>,
}

impl ReduceRunnable {
    pub fn new(
        stream_key_by: Option<StreamOperator<dyn KeySelectorFunction>>,
        stream_reduce: StreamOperator<dyn ReduceFunction>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        ReduceRunnable {
            task_id: None,
            task_number: 0,
            dependency_parallelism: 0,
            stream_key_by,
            stream_reduce,
            next_runnable,
            state: None,
            current_checkpoint_id: 0,
            reached_barriers: Vec::new(),
            max_watermark_status_timestamp: 0,
            watermark_align: None,
            limited_watermark_window: None,
            counter: Arc::new(AtomicU64::new(0)),
            expire_counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Runnable for ReduceRunnable {
    fn open(&mut self, context: &RunnableContext) {
        self.task_id = Some(context.task_descriptor.task_id.clone());
        self.next_runnable.as_mut().unwrap().open(context);

        let fun_context = context.to_fun_context();
        self.stream_reduce.operator_fn.open(&fun_context);
        self.stream_key_by
            .as_mut()
            .map(|s| s.operator_fn.open(&fun_context));

        self.task_number = context.task_descriptor.task_number;
        self.dependency_parallelism = context.task_descriptor.dependency_parallelism;

        self.watermark_align = Some(WatermarkAlign::new());

        info!(
            "ReduceRunnable Opened. task_number={}, num_tasks={}",
            self.task_number, context.task_descriptor.num_tasks
        );

        let state_mode = context
            .job_descriptor
            .job_manager
            .job_properties
            .get_keyed_state_backend()
            .unwrap_or(KeyedStateBackend::Memory);

        self.state = Some(WindowStateWrap::new(
            context.job_descriptor.job_manager.job_id.clone(),
            context.task_descriptor.chain_id,
            context.task_descriptor.task_number,
            state_mode,
        ));

        let tags = vec![
            Tag(
                "chain_id".to_string(),
                context.task_descriptor.chain_id.to_string(),
            ),
            Tag(
                "partition_num".to_string(),
                context.task_descriptor.task_number.to_string(),
            ),
        ];
        let fn_name = self.stream_reduce.operator_fn.as_ref().get_name();

        let metric_name = format!("Reduce_{}", fn_name);
        register_counter(metric_name.as_str(), tags.clone(), self.counter.clone());

        let metric_name = format!("Reduce_Expire_{}", fn_name);
        register_counter(metric_name.as_str(), tags, self.expire_counter.clone());
    }

    fn run(&mut self, element: Element) {
        let state = self.state.as_mut().unwrap();
        match element {
            Element::Record(mut record) => {
                // Record expiration check
                let acceptable = self
                    .limited_watermark_window
                    .as_ref()
                    .map(|limit_window| {
                        record
                            .get_max_location_windows()
                            .map(|window| window.min_timestamp() >= limit_window.min_timestamp())
                            .unwrap_or(true)
                    })
                    .unwrap_or(true);
                if !acceptable {
                    let n = self.expire_counter.fetch_add(1, Ordering::Relaxed);
                    if n & 1048575 == 1 {
                        error!(
                            "expire data. record window={:?}, limit window={:?}",
                            record.get_min_location_windows().unwrap(),
                            self.limited_watermark_window.as_ref().unwrap()
                        );
                    }
                    return;
                }

                let key = match &self.stream_key_by {
                    Some(stream_key_by) => stream_key_by.operator_fn.get_key(record.borrow_mut()),
                    None => Record::with_capacity(0),
                };

                let reduce_func = &self.stream_reduce.operator_fn;
                state.merge(key, record, |val1, val2| reduce_func.reduce(val1, val2));

                self.counter.fetch_add(1, Ordering::Relaxed);
            }
            Element::Watermark(watermark) => {
                let watermark_status_timestamp = watermark.status_timestamp;

                let watermark_align = self.watermark_align.as_mut().unwrap();
                watermark_align.insert(watermark);

                if watermark_status_timestamp < self.max_watermark_status_timestamp {
                    return;
                }

                self.max_watermark_status_timestamp = watermark_status_timestamp;
                let align_watermarks = watermark_align.align();

                if align_watermarks.len() > 0 {
                    let mut align_watermark = align_watermarks[align_watermarks.len() - 1].clone();
                    let minimum_watermark_window =
                        align_watermark.get_min_location_windows().unwrap();
                    self.limited_watermark_window = Some(minimum_watermark_window.clone());

                    // info!("minimum_watermark_window: {:?}", minimum_watermark_window);

                    let mut drop_windows = Vec::new();

                    // info!("begin drop window");
                    for window in state.windows() {
                        // info!(
                        //     "check window [{}/{}]",
                        //     timestamp_str(window.min_timestamp()),
                        //     timestamp_str(window.max_timestamp()),
                        // );
                        if window.max_timestamp() <= minimum_watermark_window.min_timestamp() {
                            drop_windows.push(window.clone());
                            state.drop_window(&window);

                            // info!(
                            //     "drop window [{}/{}]",
                            //     timestamp_str(window.min_timestamp()),
                            //     timestamp_str(window.max_timestamp()),
                            // );
                        }
                    }

                    drop_windows.sort_by_key(|w| w.max_timestamp());

                    if drop_windows.len() > 0 {
                        debug!(
                            "check window for drop, trigger watermark={}, drop window size={}",
                            timestamp_str(minimum_watermark_window.max_timestamp()),
                            drop_windows.len()
                        );

                        align_watermark.drop_windows = Some(drop_windows);
                        self.next_runnable
                            .as_mut()
                            .unwrap()
                            .run(Element::from(align_watermark));
                    }
                }
            }
            Element::Barrier(barrier) => {
                if self.current_checkpoint_id == 0 {
                    self.current_checkpoint_id = barrier.checkpoint_id;
                }

                if self.current_checkpoint_id == barrier.checkpoint_id {
                    self.reached_barriers.push(barrier);
                    if self.reached_barriers.len() == self.dependency_parallelism as usize {
                        self.checkpoint(self.current_checkpoint_id);
                    }

                    self.current_checkpoint_id = 0;
                    self.reached_barriers.clear();
                } else {
                    if self.current_checkpoint_id > barrier.checkpoint_id {
                        error!(
                            "Unusual state of Checkpoint. Barrier's `checkpoint_id` is less than `current_checkpoint_id`"
                        )
                    } else {
                        error!(
                            "Found a new checkpoint({}) if the current checkpoint({}) is not completed",
                            barrier.checkpoint_id,
                            self.current_checkpoint_id,
                        );

                        self.current_checkpoint_id = barrier.checkpoint_id;
                        self.reached_barriers.clear();
                        self.reached_barriers.push(barrier.clone());
                    }
                }
            }
            _ => {}
        }
    }

    fn close(&mut self) {
        self.stream_key_by.as_mut().map(|s| s.operator_fn.close());
        self.stream_reduce.operator_fn.close();
        self.next_runnable.as_mut().unwrap().close();
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, _checkpoint_id: u64) {
        // foreach self.reached_barriers
    }
}

/// a batch window aggregation
#[derive(Debug, Clone)]
pub struct WatermarkAggregation {
    /// min watermark timestamp in a window batch
    min_watermark: Option<Watermark>,
    /// min watermark status timestamp(window timestamp) in a window batch
    min_window_timestamp: u64,
    /// has reached watermarks counter in a window batch
    reached_counter: u16,
}

impl WatermarkAggregation {
    pub fn new() -> Self {
        WatermarkAggregation {
            min_watermark: None,
            min_window_timestamp: 0,
            reached_counter: 0,
        }
    }
}

/// align window through the watermark that has reached
///
/// the `status_timestamp` is the `Watermark::status_timestamp` come from `StreamStatus::timestamp`
#[derive(Debug)]
pub struct WatermarkAlign {
    // Hash<Watermark::status_timestamp, WatermarkAggregation>
    window_watermarks: HashMap<u64, WatermarkAggregation>,

    dependency_parallelism: u16,
    timeout_ms: u64,
    max_status_timestamp: u64,
}

impl WatermarkAlign {
    pub fn new() -> Self {
        WatermarkAlign {
            window_watermarks: HashMap::with_capacity(32),
            dependency_parallelism: 0,
            timeout_ms: 0,
            max_status_timestamp: 0,
        }
    }

    #[allow(dead_code)]
    pub fn with_timeout(timeout_ms: u64) -> Self {
        WatermarkAlign {
            window_watermarks: HashMap::with_capacity(32),
            dependency_parallelism: 0,
            timeout_ms,
            max_status_timestamp: 0,
        }
    }

    pub fn insert(&mut self, watermark: Watermark) {
        if self.dependency_parallelism == 0 {
            self.dependency_parallelism = watermark.num_tasks;
            self.timeout_ms = {
                let window = watermark.get_min_location_windows().unwrap();
                (window.max_timestamp() - window.min_timestamp()) * 2
            }
        }

        if watermark.num_tasks != self.dependency_parallelism {
            error!(
                "unpredictable watermark `num_tasks`={}",
                watermark.num_tasks
            );
            return;
        }

        if watermark.status_timestamp > self.max_status_timestamp {
            self.max_status_timestamp = watermark.status_timestamp
        }

        let watermark_agg = self
            .window_watermarks
            .entry(watermark.status_timestamp)
            .or_insert(WatermarkAggregation::new());

        let min_window_timestamp = watermark
            .get_min_location_windows()
            .unwrap()
            .min_timestamp();

        if watermark_agg.min_watermark.is_none() {
            watermark_agg.min_watermark = Some(watermark);
            watermark_agg.min_window_timestamp = min_window_timestamp;
        } else {
            // update min watermark
            if min_window_timestamp < watermark_agg.min_window_timestamp {
                watermark_agg.min_watermark = Some(watermark);
                watermark_agg.min_window_timestamp = min_window_timestamp;
            }
        }

        watermark_agg.reached_counter += 1;
    }

    /// check all has aligned watermarks
    fn check_aligned(&mut self) -> Vec<u64> {
        let align_watermarks: Vec<u64> = self
            .window_watermarks
            .iter()
            .filter(|(_status_timestamp, watermark_agg)| {
                watermark_agg.reached_counter == self.dependency_parallelism
            })
            .map(|(status_timestamp, _watermark_agg)| *status_timestamp)
            .collect();

        align_watermarks
    }

    /// check all unaligned watermark overflow capacity limits
    fn check_unaligned(&mut self) -> Vec<u64> {
        if self.window_watermarks.len() < 100 {
            return vec![];
        }

        let mut status_timestamp_vec: Vec<u64> = self
            .window_watermarks
            .keys()
            .map(|status_timestamp| *status_timestamp)
            .collect();
        status_timestamp_vec.sort();

        (&status_timestamp_vec[0..status_timestamp_vec.len() - 100]).to_vec()
    }

    #[allow(dead_code)]
    pub fn aligned(&mut self) -> Option<Watermark> {
        let mut align_watermarks = self.check_aligned();
        if align_watermarks.len() == 0 {
            align_watermarks = self.check_unaligned();
        }

        let watermark_agg_vec: Vec<WatermarkAggregation> = align_watermarks
            .iter()
            .map(|status_timestamp| self.window_watermarks.remove(status_timestamp).unwrap())
            .collect();

        watermark_agg_vec
            .iter()
            .max_by(|x, y| x.min_window_timestamp.cmp(&y.min_window_timestamp))
            .map(|x| x.min_watermark.as_ref().unwrap().clone())
    }

    pub fn align(&mut self) -> Vec<Watermark> {
        // a maximum window that satisfies drop condition
        let mut max_drop_watermark_agg = None;
        let mut drop_watermarks = Vec::new();
        for (status_timestamp, watermark_agg) in &self.window_watermarks {
            if self.max_status_timestamp - status_timestamp > self.timeout_ms
                || watermark_agg.reached_counter == self.dependency_parallelism
            {
                drop_watermarks.push(*status_timestamp);

                if max_drop_watermark_agg.is_none() {
                    max_drop_watermark_agg = Some(watermark_agg.clone());
                } else {
                    // update max watermark
                    if watermark_agg.min_window_timestamp
                        > max_drop_watermark_agg
                            .as_ref()
                            .unwrap()
                            .min_window_timestamp
                    {
                        max_drop_watermark_agg = Some(watermark_agg.clone());
                    }
                }
            }
        }

        debug!(
            "WatermarkAggregation size: {}",
            self.window_watermarks.len()
        );

        let max_watermark = match max_drop_watermark_agg {
            Some(watermark_agg) => vec![watermark_agg.min_watermark.as_ref().unwrap().clone()],
            None => vec![],
        };

        for status_timestamp in drop_watermarks {
            debug!("remove status_timestamp {}", status_timestamp);
            self.window_watermarks.remove(&status_timestamp);
        }

        max_watermark
    }
}

#[cfg(test)]
mod tests {
    use crate::api::element::{StreamStatus, Watermark};
    use crate::api::window::{TimeWindow, WindowWrap};
    use crate::runtime::worker::runnable::reduce_runnable::WatermarkAlign;

    #[test]
    pub fn watermark_align_test() {
        let stream_statue = StreamStatus::new(1, false);
        // 2020-07-01 15:00:00:000
        let base_timestamp = 1593586800000u64;
        let timeout_ms = 9000;
        let num_tasks = 3;

        let mut align = WatermarkAlign::with_timeout(timeout_ms);
        {
            let timestamp = base_timestamp;
            let mut watermark = Watermark::new(0, num_tasks, timestamp, &stream_statue);
            let window = TimeWindow::new(timestamp, timestamp + 3000);
            watermark.set_location_windows(vec![WindowWrap::TimeWindow(window)]);

            align.insert(watermark);
        }

        {
            let timestamp = base_timestamp + 1000;
            let mut watermark = Watermark::new(1, num_tasks, timestamp, &stream_statue);
            let window = TimeWindow::new(timestamp, timestamp + 3000);
            watermark.set_location_windows(vec![WindowWrap::TimeWindow(window)]);

            align.insert(watermark);
        }

        {
            let timestamp = base_timestamp + 2000;
            let mut watermark = Watermark::new(2, num_tasks, timestamp, &stream_statue);
            let window = TimeWindow::new(timestamp, timestamp + 3000);
            watermark.set_location_windows(vec![WindowWrap::TimeWindow(window)]);

            align.insert(watermark);
        }

        let watermarks = align.align();
        assert_eq!(watermarks.len(), 1);
        assert_eq!(watermarks[0].timestamp, base_timestamp);
    }

    #[test]
    pub fn watermark_align_delay_test() {
        let stream_statue = StreamStatus::new(1, false);
        // 2020-07-01 15:00:00:000
        let base_timestamp = 1593586800000u64;
        let timeout_ms = 9000;
        let num_tasks = 3;

        let mut align = WatermarkAlign::with_timeout(timeout_ms);
        {
            let timestamp = base_timestamp;
            let mut watermark = Watermark::new(0, num_tasks, timestamp, &stream_statue);
            let window = TimeWindow::new(timestamp, timestamp + 3000);
            watermark.set_location_windows(vec![WindowWrap::TimeWindow(window)]);

            align.insert(watermark);
        }

        {
            let timestamp = base_timestamp + 1000;
            let mut watermark = Watermark::new(1, num_tasks, timestamp, &stream_statue);
            let window = TimeWindow::new(timestamp, timestamp + 3000);
            watermark.set_location_windows(vec![WindowWrap::TimeWindow(window)]);

            align.insert(watermark);
        }

        let watermarks = align.align();
        assert_eq!(watermarks.len(), 0);
    }

    #[test]
    pub fn watermark_align_expired_test() {
        let stream_statue = StreamStatus::new(1, false);
        // 2020-07-01 15:00:00:000
        let base_timestamp = 1593586800000u64;
        let timeout_ms = 9000;
        let num_tasks = 3;

        let mut align = WatermarkAlign::with_timeout(timeout_ms);
        {
            let timestamp = base_timestamp;
            let mut watermark = Watermark::new(0, num_tasks, timestamp, &stream_statue);
            let window = TimeWindow::new(timestamp, timestamp + 3000);
            watermark.set_location_windows(vec![WindowWrap::TimeWindow(window)]);

            align.insert(watermark);
        }

        {
            let timestamp = base_timestamp + 1000 + timeout_ms;
            let mut watermark = Watermark::new(1, num_tasks, timestamp, &stream_statue);
            let window = TimeWindow::new(timestamp, timestamp + 3000);
            watermark.set_location_windows(vec![WindowWrap::TimeWindow(window)]);

            align.insert(watermark);
        }

        let watermarks = align.align();
        assert_eq!(watermarks.len(), 1);
        assert_eq!(watermarks[0].timestamp, base_timestamp);
    }

    #[test]
    pub fn watermark_align_expired1_test() {
        // 2020-07-01 15:00:00:000
        let base_timestamp = 1593586800000u64;
        let timeout_ms = 9000;
        let num_tasks = 3;

        let mut align = WatermarkAlign::with_timeout(timeout_ms);
        {
            let stream_statue = StreamStatus::new(1, false);

            let timestamp = base_timestamp;
            let mut watermark = Watermark::new(0, num_tasks, timestamp, &stream_statue);
            let window = TimeWindow::new(timestamp, timestamp + 3000);
            watermark.set_location_windows(vec![WindowWrap::TimeWindow(window)]);

            align.insert(watermark);
        }

        {
            let stream_statue = StreamStatus::new(1 + timeout_ms + 1, false);

            let timestamp = base_timestamp + 1000 + timeout_ms;
            let mut watermark = Watermark::new(1, num_tasks, timestamp, &stream_statue);
            let window = TimeWindow::new(timestamp, timestamp + 3000);
            watermark.set_location_windows(vec![WindowWrap::TimeWindow(window)]);

            align.insert(watermark);
        }

        let watermarks = align.align();
        assert_eq!(watermarks.len(), 1);
        assert_eq!(watermarks[0].timestamp, base_timestamp);
    }
}
