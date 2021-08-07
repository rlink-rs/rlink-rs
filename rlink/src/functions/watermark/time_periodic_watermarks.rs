use std::time::Duration;

use crate::core::element::Record;
use crate::core::watermark::{Watermark, WatermarkGenerator};
use crate::utils::date_time::current_timestamp_millis;

/// Determine whether to generate watermark based on the rate of processing time and event time
///
/// check condition:   event_timestamp(n) - event_timestamp(m) > event_period, (n > m)
/// trigger condition: process_timestamp(n) - process_timestamp(m) < process_period
#[derive(Debug)]
pub struct TimePeriodicWatermarks {
    watermarks: Box<dyn WatermarkGenerator>,

    process_timestamp: i64,
    event_timestamp: i64,

    process_period: i64,
    event_period: i64,
}

impl TimePeriodicWatermarks {
    pub fn new(
        watermarks: Box<dyn WatermarkGenerator>,
        process_period: Duration,
        event_period: Duration,
    ) -> Self {
        TimePeriodicWatermarks {
            watermarks,
            process_timestamp: 0,
            event_timestamp: 0,

            process_period: process_period.as_millis() as i64,
            event_period: event_period.as_millis() as i64,
        }
    }
}

impl WatermarkGenerator for TimePeriodicWatermarks {
    fn on_event(&mut self, record: &mut Record, event_timestamp: u64) -> Option<Watermark> {
        let w = self.watermarks.on_event(record, event_timestamp);
        if w.is_some() {
            return w;
        }

        let event_timestamp = event_timestamp as i64;

        if self.process_timestamp == 0 {
            self.process_timestamp = current_timestamp_millis() as i64;
            self.event_timestamp = event_timestamp;

            None
        } else {
            if event_timestamp - self.event_timestamp > self.event_period {
                let old_process_timestamp = self.process_timestamp;
                let current_process_timestamp = current_timestamp_millis() as i64;

                self.process_timestamp = current_process_timestamp;
                self.event_timestamp = event_timestamp;

                if current_process_timestamp - old_process_timestamp < self.process_period {
                    return self.watermarks.on_periodic_emit();
                }
            }

            None
        }
    }

    fn on_periodic_emit(&mut self) -> Option<Watermark> {
        self.watermarks.on_periodic_emit()
    }
}
