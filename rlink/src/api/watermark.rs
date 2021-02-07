use std::fmt::Debug;
use std::time::Duration;

use crate::api::checkpoint::CheckpointFunction;
use crate::api::element::{Record, StreamStatus};
use crate::api::function::Function;
use crate::utils::date_time::timestamp_str;

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

pub trait TimestampAssigner
where
    Self: Function + Debug,
{
    fn extract_timestamp(&mut self, row: &mut Record, previous_element_timestamp: u64) -> u64;

    fn checkpoint_function(&mut self) -> Option<Box<&mut dyn CheckpointFunction>> {
        None
    }
}

pub trait WatermarkAssigner
where
    Self: TimestampAssigner + Function + Debug,
{
    /// Return the current `Watermark` and row's timestamp
    fn watermark(&mut self, stream_status: &StreamStatus) -> Option<Watermark>;
    fn current_watermark(&self) -> Option<Watermark>;
}

#[derive(Debug)]
pub struct BoundedOutOfOrdernessTimestampExtractor<E>
where
    E: TimestampAssigner,
{
    current_max_timestamp: u64,
    previous_emitted_watermark: u64,
    last_emitted_watermark: u64,
    max_out_of_orderness: u64,
    extract_timestamp: E,
}

impl<E> BoundedOutOfOrdernessTimestampExtractor<E>
where
    E: TimestampAssigner,
{
    pub fn new(max_out_of_orderness: Duration, extract_timestamp: E) -> Self {
        let max_out_of_orderness = max_out_of_orderness.as_millis() as u64;
        BoundedOutOfOrdernessTimestampExtractor {
            current_max_timestamp: max_out_of_orderness, // Long.MIN_VALUE + this.maxOutOfOrderness;
            previous_emitted_watermark: 0,
            last_emitted_watermark: 0, // Long.MIN_VALUE
            max_out_of_orderness,
            extract_timestamp,
        }
    }
}

impl<E> WatermarkAssigner for BoundedOutOfOrdernessTimestampExtractor<E>
where
    E: TimestampAssigner,
{
    fn watermark(&mut self, _stream_status: &StreamStatus) -> Option<Watermark> {
        let potential_wm = self.current_max_timestamp - self.max_out_of_orderness;
        debug!(
            "potential_wm={}, current_max_timestamp={}, max_out_of_orderness={}",
            timestamp_str(potential_wm),
            timestamp_str(self.current_max_timestamp),
            self.max_out_of_orderness,
        );
        if potential_wm > self.last_emitted_watermark {
            self.previous_emitted_watermark = self.last_emitted_watermark;
            self.last_emitted_watermark = potential_wm;

            debug!(
                "Create Watermark: {}",
                timestamp_str(self.last_emitted_watermark)
            );
            Some(Watermark::new(self.last_emitted_watermark))
        } else {
            None
        }
    }

    fn current_watermark(&self) -> Option<Watermark> {
        if self.last_emitted_watermark == 0 {
            None
        } else {
            Some(Watermark::new(self.last_emitted_watermark))
        }
    }
}

impl<E> TimestampAssigner for BoundedOutOfOrdernessTimestampExtractor<E>
where
    E: TimestampAssigner,
{
    fn extract_timestamp(&mut self, row: &mut Record, previous_element_timestamp: u64) -> u64 {
        let timestamp = self
            .extract_timestamp
            .extract_timestamp(row, previous_element_timestamp);
        if timestamp > self.current_max_timestamp {
            self.current_max_timestamp = timestamp;
        }
        return timestamp;
    }
}

impl<E> Function for BoundedOutOfOrdernessTimestampExtractor<E>
where
    E: TimestampAssigner,
{
    fn name(&self) -> &str {
        "BoundedOutOfOrdernessTimestampExtractor"
    }
}
