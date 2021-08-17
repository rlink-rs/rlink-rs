use std::convert::TryFrom;

use crate::core::checkpoint::CheckpointFunction;
use crate::core::element::Record;
use crate::core::function::{Context, FilterFunction, NamedFunction};
use crate::core::properties::Properties;
use crate::core::window::TWindow;

pub struct RangeWindowFilter {
    window_start_timestamp: u64,
    window_stop_timestamp: u64,
}

impl RangeWindowFilter {
    pub fn new(window_start_timestamp: u64, window_stop_timestamp: u64) -> Self {
        RangeWindowFilter {
            window_start_timestamp,
            window_stop_timestamp,
        }
    }
}

impl FilterFunction for RangeWindowFilter {
    fn open(&mut self, _context: &Context) -> crate::core::Result<()> {
        Ok(())
    }

    fn filter(&self, record: &mut Record) -> bool {
        if let Some(window) = &record.trigger_window {
            if window.min_timestamp() < self.window_start_timestamp
                || window.max_timestamp() > self.window_stop_timestamp
            {
                return false;
            }
        }

        true
    }

    fn close(&mut self) -> crate::core::Result<()> {
        Ok(())
    }
}

impl NamedFunction for RangeWindowFilter {
    fn name(&self) -> &str {
        "RangeWindowFilter"
    }
}

impl CheckpointFunction for RangeWindowFilter {}

impl TryFrom<Properties> for RangeWindowFilter {
    type Error = crate::core::Error;

    fn try_from(properties: Properties) -> Result<Self, Self::Error> {
        let begin = properties.get_u64("begin").unwrap_or(0);
        let end = properties.get_u64("end").unwrap_or(u64::MAX);

        Ok(RangeWindowFilter::new(begin, end))
    }
}
