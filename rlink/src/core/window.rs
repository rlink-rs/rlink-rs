use std::cmp::{max, min};
use std::fmt::Debug;

use crate::core::checkpoint::CheckpointFunction;
use crate::core::function::NamedFunction;
use crate::utils;

pub trait TWindow: Debug + Clone {
    fn max_timestamp(&self) -> u64;
    fn min_timestamp(&self) -> u64;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub struct TimeWindow {
    start: u64,
    end: u64,
}

impl TimeWindow {
    pub fn new(start: u64, end: u64) -> Self {
        TimeWindow { start, end }
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> u64 {
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

    pub fn get_window_start_with_offset(timestamp: u64, offset: i64, window_size: u64) -> i64 {
        let timestamp = timestamp as i64;
        let window_size = window_size as i64;
        timestamp - (timestamp - offset + window_size) % window_size
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

impl Default for Window {
    fn default() -> Self {
        Window::TimeWindow(TimeWindow::default())
    }
}

#[derive(Debug)]
pub struct WindowAssignerContext {}

impl WindowAssignerContext {
    pub fn current_processing_time(&self) -> u64 {
        utils::date_time::current_timestamp().as_millis() as u64
    }
}

/// A `WindowAssigner` assigns zero or more `Window`s to an element.
pub trait WindowAssigner
where
    Self: NamedFunction + CheckpointFunction + Debug + Send + Sync,
{
    /// Returns a collection of windows that should be assigned to the element.
    fn assign_windows(&self, timestamp: u64, context: WindowAssignerContext) -> Vec<Window>;
}
