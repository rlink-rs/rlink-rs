use std::time::Duration;

use crate::core::element::Record;
use crate::core::watermark::{Watermark, WatermarkGenerator, MIN_WATERMARK};

#[derive(Debug)]
pub struct BoundedOutOfOrdernessWatermarks {
    max_timestamp: u64,
    out_of_orderness_millis: u64,
}

impl BoundedOutOfOrdernessWatermarks {
    pub fn new(out_of_orderness_millis: Duration) -> Self {
        let out_of_orderness_millis = out_of_orderness_millis.as_millis() as u64;

        BoundedOutOfOrdernessWatermarks {
            max_timestamp: 0,
            out_of_orderness_millis,
        }
    }
}

impl WatermarkGenerator for BoundedOutOfOrdernessWatermarks {
    fn on_event(&mut self, _record: &mut Record, event_timestamp: u64) -> Option<Watermark> {
        if event_timestamp > self.max_timestamp {
            self.max_timestamp = event_timestamp;
        }
        None
    }

    fn on_periodic_emit(&mut self) -> Option<Watermark> {
        let timestamp = if self.max_timestamp > self.out_of_orderness_millis {
            self.max_timestamp - self.out_of_orderness_millis - 1
        } else {
            MIN_WATERMARK.timestamp
        };
        Some(Watermark::new(timestamp))
    }
}
