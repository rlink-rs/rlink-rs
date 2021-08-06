use std::fmt::Debug;

use crate::core::checkpoint::CheckpointFunction;
use crate::core::element::Record;
use crate::core::function::NamedFunction;

pub const MAX_WATERMARK: Watermark = Watermark {
    timestamp: 253402185600000u64,
};
pub const MIN_WATERMARK: Watermark = Watermark { timestamp: 0x0 };

#[derive(Clone, Debug)]
pub struct Watermark {
    pub(crate) timestamp: u64,
}

impl Watermark {
    pub fn new(timestamp: u64) -> Self {
        Watermark { timestamp }
    }
}

impl PartialEq for Watermark {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp == other.timestamp
    }
}

pub trait TimestampAssigner: Debug {
    fn extract_timestamp(&mut self, row: &mut Record, previous_element_timestamp: u64) -> u64;
}

pub trait WatermarkGenerator: Debug {
    /// Called for every event, allows the watermark generator to examine and remember the event
    /// timestamps, or to emit a watermark based on the event itself.
    fn on_event(&mut self, record: &mut Record, event_timestamp: u64) -> Option<Watermark>;

    /// Called periodically, and might emit a new watermark, or not.
    ///
    /// The interval in which this method is called and Watermarks are generated depends on
    /// `StreamStatus` interval
    fn on_periodic_emit(&mut self) -> Option<Watermark>;
}

pub trait WatermarkStrategy: NamedFunction + CheckpointFunction + Debug {
    fn create_watermark_generator(&mut self) -> Box<dyn WatermarkGenerator>;
    fn create_timestamp_assigner(&mut self) -> Box<dyn TimestampAssigner>;
}
