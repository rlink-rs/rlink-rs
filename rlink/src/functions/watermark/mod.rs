use std::fmt::{Debug, Formatter};
use std::time::Duration;

use crate::core::checkpoint::CheckpointFunction;
use crate::core::element::Record;
use crate::core::function::NamedFunction;
use crate::core::watermark::{
    TimestampAssigner, Watermark, WatermarkGenerator, WatermarkStrategy, MIN_WATERMARK,
};
use crate::utils::date_time::current_timestamp_millis;

#[derive(Debug)]
pub struct SchemaTimestampAssigner {
    field_types: Vec<u8>,
    column: usize,
}

impl SchemaTimestampAssigner {
    pub fn new(column: usize, field_types: &[u8]) -> Self {
        SchemaTimestampAssigner {
            column,
            field_types: field_types.to_vec(),
        }
    }
}

impl TimestampAssigner for SchemaTimestampAssigner {
    fn extract_timestamp(&mut self, row: &mut Record, _previous_element_timestamp: u64) -> u64 {
        let reader = row.as_reader(self.field_types.as_slice());
        reader.get_u64(self.column).unwrap()
    }
}

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

/// Determine whether to generate watermark based on the rate of processing time and event time
///
/// check condition:   event_timestamp(n) - event_timestamp(m) > event_period, (n > m)
/// trigger condition: process_timestamp(n) - process_timestamp(m) < process_period
#[derive(Debug)]
pub struct TimePeriodicWatermarks {
    inner: Box<dyn WatermarkGenerator>,

    process_timestamp: i64,
    event_timestamp: i64,

    process_period: i64,
    event_period: i64,
}

impl TimePeriodicWatermarks {
    pub fn new(
        inner: Box<dyn WatermarkGenerator>,
        process_period: Duration,
        event_period: Duration,
    ) -> Self {
        TimePeriodicWatermarks {
            inner,
            process_timestamp: 0,
            event_timestamp: 0,

            process_period: process_period.as_millis() as i64,
            event_period: event_period.as_millis() as i64,
        }
    }
}

impl WatermarkGenerator for TimePeriodicWatermarks {
    fn on_event(&mut self, record: &mut Record, event_timestamp: u64) -> Option<Watermark> {
        let w = self.inner.on_event(record, event_timestamp);
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
                    return self.inner.on_periodic_emit();
                }
            }

            None
        }
    }

    fn on_periodic_emit(&mut self) -> Option<Watermark> {
        self.inner.on_periodic_emit()
    }
}

pub struct DefaultWatermarkStrategy {
    watermark_generator: Option<Box<dyn WatermarkGenerator>>,
    timestamp_assigner: Option<Box<dyn TimestampAssigner>>,
}

impl DefaultWatermarkStrategy {
    pub fn new() -> Self {
        DefaultWatermarkStrategy {
            watermark_generator: None,
            timestamp_assigner: None,
        }
    }

    pub fn for_bounded_out_of_orderness(mut self, out_of_orderness_millis: Duration) -> Self {
        self.watermark_generator = Some(Box::new(BoundedOutOfOrdernessWatermarks::new(
            out_of_orderness_millis,
        )));
        self
    }

    pub fn wrap_time_periodic(mut self, process_period: Duration, event_period: Duration) -> Self {
        if let Some(generator) = self.watermark_generator.take() {
            self.watermark_generator = Some(Box::new(TimePeriodicWatermarks::new(
                generator,
                process_period,
                event_period,
            )));
            self
        } else {
            panic!("no WatermarkGenerator for wrapper");
        }
    }

    pub fn for_schema_timestamp_assigner(mut self, column: usize, field_types: &[u8]) -> Self {
        self.timestamp_assigner = Some(Box::new(SchemaTimestampAssigner::new(column, field_types)));
        self
    }

    pub fn for_watermark_generator<T>(mut self, generator: T) -> Self
    where
        T: WatermarkGenerator + 'static,
    {
        self.watermark_generator = Some(Box::new(generator));
        self
    }

    pub fn for_timestamp_assigner<T>(mut self, assigner: T) -> Self
    where
        T: TimestampAssigner + 'static,
    {
        self.timestamp_assigner = Some(Box::new(assigner));
        self
    }
}

impl WatermarkStrategy for DefaultWatermarkStrategy {
    fn create_watermark_generator(&mut self) -> Box<dyn WatermarkGenerator> {
        self.watermark_generator.take().unwrap()
    }

    fn create_timestamp_assigner(&mut self) -> Box<dyn TimestampAssigner> {
        self.timestamp_assigner.take().unwrap()
    }
}

impl NamedFunction for DefaultWatermarkStrategy {
    fn name(&self) -> &str {
        "DefaultWatermarkStrategy"
    }
}

impl CheckpointFunction for DefaultWatermarkStrategy {}

impl Debug for DefaultWatermarkStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DefaultWatermarkStrategy")
    }
}
