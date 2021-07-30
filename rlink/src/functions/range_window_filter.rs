use std::str::FromStr;

use crate::core::checkpoint::CheckpointFunction;
use crate::core::element::Record;
use crate::core::function::{Context, FilterFunction, NamedFunction};
use crate::core::properties::Properties;
use crate::core::window::TWindow;

pub const FN_WINDOW_START_TIMESTAMP: &'static str = "FN_WINDOW_START_TIMESTAMP";
pub const FN_WINDOW_STOP_TIMESTAMP: &'static str = "FN_WINDOW_STOP_TIMESTAMP";

pub trait RangeWindowProperties {
    fn set_window_start_timestamp(&mut self, window_start_timestamp: u64);
    fn get_window_start_timestamp(&self) -> anyhow::Result<u64>;

    fn set_window_stop_timestamp(&mut self, window_start_timestamp: u64);
    fn get_window_stop_timestamp(&self) -> anyhow::Result<u64>;
}

impl RangeWindowProperties for Properties {
    fn set_window_start_timestamp(&mut self, window_start_timestamp: u64) {
        let value = window_start_timestamp.to_string();
        self.set_string(FN_WINDOW_START_TIMESTAMP.to_string(), value);
    }

    fn get_window_start_timestamp(&self) -> anyhow::Result<u64> {
        let value = self.get_string(FN_WINDOW_START_TIMESTAMP)?;
        u64::from_str(value.as_str()).map_err(|e| anyhow!(e))
    }

    fn set_window_stop_timestamp(&mut self, window_start_timestamp: u64) {
        let value = window_start_timestamp.to_string();
        self.set_string(FN_WINDOW_STOP_TIMESTAMP.to_string(), value);
    }

    fn get_window_stop_timestamp(&self) -> anyhow::Result<u64> {
        let value = self.get_string(FN_WINDOW_STOP_TIMESTAMP)?;
        u64::from_str(value.as_str()).map_err(|e| anyhow!(e))
    }
}

pub struct RangeWindowFilter {
    window_start_timestamp: u64,
    window_stop_timestamp: u64,
}

impl RangeWindowFilter {
    pub fn new() -> Self {
        RangeWindowFilter {
            window_start_timestamp: 0,
            window_stop_timestamp: u64::MAX,
        }
    }
}

impl FilterFunction for RangeWindowFilter {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        let prop = &context.application_properties;
        if let Ok(v) = prop.get_u64(FN_WINDOW_START_TIMESTAMP) {
            self.window_start_timestamp = v;
        }
        if let Ok(v) = prop.get_u64(FN_WINDOW_STOP_TIMESTAMP) {
            self.window_stop_timestamp = v;
        }

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
