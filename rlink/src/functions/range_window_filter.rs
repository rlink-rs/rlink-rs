use std::str::FromStr;

use crate::core::checkpoint::CheckpointFunction;
use crate::core::element::Record;
use crate::core::function::{Context, FilterFunction, NamedFunction};
use crate::core::properties::Properties;
use crate::core::window::TWindow;
use crate::utils::process::parse_arg;

pub const WINDOW_START_TIMESTAMP: &'static str = "WINDOW_START_TIMESTAMP";
pub const WINDOW_STOP_TIMESTAMP: &'static str = "WINDOW_STOP_TIMESTAMP";

pub fn init_from_args(properties: &mut Properties) {
    if let Ok(v) = parse_arg(WINDOW_START_TIMESTAMP.to_lowercase().as_str()) {
        let v = u64::from_str(v.as_str()).unwrap();
        properties.set_u64(WINDOW_START_TIMESTAMP, v);
    }

    if let Ok(v) = parse_arg(WINDOW_STOP_TIMESTAMP.to_lowercase().as_str()) {
        let v = u64::from_str(v.as_str()).unwrap();
        properties.set_u64(WINDOW_STOP_TIMESTAMP, v);
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
        if let Ok(v) = prop.get_u64(WINDOW_START_TIMESTAMP) {
            self.window_start_timestamp = v;
        }
        if let Ok(v) = prop.get_u64(WINDOW_STOP_TIMESTAMP) {
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
