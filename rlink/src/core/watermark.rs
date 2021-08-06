use std::fmt::Debug;

use crate::core::checkpoint::CheckpointFunction;
use crate::core::element::Record;
use crate::core::function::NamedFunction;

pub const MAX_WATERMARK: Watermark = Watermark {
    timestamp: 253402185600000u64,
};
pub const MIN_WATERMARK: Watermark = Watermark { timestamp: 0x0 };

/// Watermarks are the progress indicators in the data streams. A watermark signifies that no events
/// with a timestamp smaller or equal to the watermark's time will occur after the water. A
/// watermark with timestamp T indicates that the stream's event time has progressed to time T.
///
/// Watermarks are created at the `WatermarkAssigner` and propagate through the streams and
/// operators.
///
/// Note: A stream's time starts with a watermark of `0u64`.
#[derive(Clone, Debug)]
pub struct Watermark {
    /// The timestamp of the watermark in milliseconds.
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

/// A `TimestampAssigner` assigns event time timestamps to elements. These timestamps are used by
/// all functions that operate on event time, for example event time windows.
///
/// Timestamps can be an arbitrary `u64` value, but all built-in implementations represent it as the
/// milliseconds since the Epoch (midnight, January 1, 1970 UTC).
pub trait TimestampAssigner: Debug {
    /// Assigns a timestamp to an element, in milliseconds since the Epoch. This is independent of
    /// any particular time zone or calendar.
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

/// The WatermarkStrategy defines how to generate `Watermark`s in the stream sources. The
/// WatermarkStrategy is a builder/factory for the `WatermarkGenerator` that generates the watermarks
/// and the `TimestampAssigner` which assigns the internal timestamp of a record.
pub trait WatermarkStrategy: NamedFunction + CheckpointFunction + Debug {
    /// Instantiates a `WatermarkGenerator` that generates watermarks according to this strategy.
    fn create_watermark_generator(&mut self) -> Box<dyn WatermarkGenerator>;

    /// Instantiates a `TimestampAssigner` for assigning timestamps according to this strategy.
    fn create_timestamp_assigner(&mut self) -> Box<dyn TimestampAssigner>;
}
