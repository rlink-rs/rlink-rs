use std::cmp::{max, min};
use std::fmt::Debug;
use std::time::Duration;

use crate::api::function::Function;
use crate::utils;

pub trait TWindow: Debug + Clone {
    fn max_timestamp(&self) -> u64;
    fn min_timestamp(&self) -> u64;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TimeWindow {
    start: u64,
    end: u64,
}

impl TimeWindow {
    pub fn new(start: u64, end: u64) -> Self {
        TimeWindow { start, end }
    }

    pub fn get_start(&self) -> u64 {
        self.start
    }

    pub fn get_end(&self) -> u64 {
        self.end
    }

    /// Returns `true` if this window intersects the given window.
    pub fn intersects(&self, other: TimeWindow) -> bool {
        self.start <= other.end && self.end >= other.start
    }

    /// Returns the minimal window covers both this window and the given window.
    pub fn cover(&self, other: TimeWindow) -> TimeWindow {
        TimeWindow::new(min(self.start, other.start), max(self.end, other.end))
    }

    // pub fn mergeWindows(windows: Vec<TimeWindow>, )

    pub fn get_window_start_with_offset(timestamp: u64, offset: i64, window_size: u64) -> u64 {
        let timestamp = timestamp as i64;
        let window_size = window_size as i64;
        (timestamp - (timestamp - offset + window_size) % window_size) as u64
    }
}

impl TWindow for TimeWindow {
    fn max_timestamp(&self) -> u64 {
        self.end
    }

    fn min_timestamp(&self) -> u64 {
        self.start
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Window {
    TimeWindow(TimeWindow),
}

impl TWindow for Window {
    fn max_timestamp(&self) -> u64 {
        match self {
            Window::TimeWindow(time_window) => time_window.max_timestamp(),
        }
    }

    fn min_timestamp(&self) -> u64 {
        match self {
            Window::TimeWindow(time_window) => time_window.min_timestamp(),
        }
    }
}

#[derive(Debug)]
pub struct WindowAssignerContext {}

impl WindowAssignerContext {
    pub fn get_current_processing_time(&self) -> u64 {
        utils::date_time::current_timestamp().as_millis() as u64
    }
}

pub trait WindowAssigner
where
    Self: Function + Debug,
{
    fn assign_windows(&self, timestamp: u64, context: WindowAssignerContext) -> Vec<Window>;
}

#[derive(Debug)]
pub struct SlidingEventTimeWindows {
    size: u64,
    slide: u64,
    offset: i64,
}

impl SlidingEventTimeWindows {
    pub fn new(size: Duration, slide: Duration, offset: Option<Duration>) -> Self {
        let size = size.as_millis() as u64;
        let slide = slide.as_millis() as u64;
        let offset = offset.map(|x| x.as_millis() as i64).unwrap_or(0);

        if offset.abs() as u64 >= slide || size <= 0 {
            panic!(
                "SlidingEventTimeWindows parameters must satisfy offset.abs() < slide and size > 0"
            )
        }
        SlidingEventTimeWindows {
            size,
            slide,
            offset,
        }
    }
}

impl WindowAssigner for SlidingEventTimeWindows {
    fn assign_windows(&self, timestamp: u64, _context: WindowAssignerContext) -> Vec<Window> {
        let mut windows = Vec::with_capacity((self.size / self.slide) as usize);
        let last_start =
            TimeWindow::get_window_start_with_offset(timestamp, self.offset, self.slide);

        let mut start = last_start;
        loop {
            if start > timestamp - self.size {
                let window = TimeWindow::new(start, start + self.size);
                // info!("Create window: {}", window);
                windows.push(Window::TimeWindow(window));
                start -= self.slide;
            } else {
                break;
            }
        }

        windows.sort_by_key(|x| x.min_timestamp());
        windows
    }
}

impl Function for SlidingEventTimeWindows {
    fn get_name(&self) -> &str {
        "SlidingEventTimeWindows"
    }
}
