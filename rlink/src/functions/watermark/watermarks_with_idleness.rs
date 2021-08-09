use std::time::Duration;

use crate::core::element::Record;
use crate::core::watermark::{Watermark, WatermarkGenerator, IDLE_WATERMARK};
use crate::utils::date_time::current_timestamp_millis;

/// A `WatermarkGenerator` that adds idleness detection to another `WatermarkGenerator`. If no
/// events come within a certain time (timeout duration) then this generator marks the stream as
/// idle, until the next watermark is generated.
///
/// Reference: `org.apache.flink.api.common.eventtime.WatermarksWithIdleness`
#[derive(Debug)]
pub struct WatermarksWithIdleness {
    watermarks: Box<dyn WatermarkGenerator>,
    idleness_timer: IdlenessTimer,
}

impl WatermarksWithIdleness {
    pub fn new(watermarks: Box<dyn WatermarkGenerator>, idle_timeout: Duration) -> Self {
        WatermarksWithIdleness {
            watermarks,
            idleness_timer: IdlenessTimer::new(idle_timeout),
        }
    }
}

impl WatermarkGenerator for WatermarksWithIdleness {
    fn on_event(&mut self, record: &mut Record, event_timestamp: u64) -> Option<Watermark> {
        self.idleness_timer.activity();
        self.watermarks.on_event(record, event_timestamp)
    }

    fn on_periodic_emit(&mut self) -> Option<Watermark> {
        if self.idleness_timer.check_if_idle() {
            Some(IDLE_WATERMARK)
        } else {
            self.watermarks.on_periodic_emit()
        }
    }
}

#[derive(Debug)]
pub struct IdlenessTimer {
    counter: u64,
    last_counter: u64,
    start_of_inactivity_millis: u64,
    max_idle_time_millis: u64,
}

impl IdlenessTimer {
    pub fn new(idle_timeout: Duration) -> Self {
        IdlenessTimer {
            counter: 0,
            last_counter: 0,
            start_of_inactivity_millis: 0,
            max_idle_time_millis: idle_timeout.as_millis() as u64,
        }
    }

    pub fn activity(&mut self) {
        self.counter += 1;
    }

    pub fn check_if_idle(&mut self) -> bool {
        if self.counter != self.last_counter {
            // activity since the last check. we reset the timer
            self.last_counter = self.counter;
            self.start_of_inactivity_millis = 0u64;
            false
        } else if self.start_of_inactivity_millis == 0u64 {
            // timer started but has not yet reached idle timeout

            // first time that we see no activity since the last periodic probe
            // begin the timer
            self.start_of_inactivity_millis = current_timestamp_millis();
            false
        } else {
            current_timestamp_millis() - self.start_of_inactivity_millis > self.max_idle_time_millis
        }
    }
}
