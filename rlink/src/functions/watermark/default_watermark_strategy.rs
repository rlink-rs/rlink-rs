use std::fmt::{Debug, Formatter};
use std::time::Duration;

use crate::core::checkpoint::CheckpointFunction;
use crate::core::function::NamedFunction;
use crate::core::watermark::{TimestampAssigner, WatermarkGenerator, WatermarkStrategy};
use crate::functions::column_locate::ColumnLocateBuilder;
use crate::functions::watermark::watermarks_with_idleness::WatermarksWithIdleness;
use crate::functions::watermark::{
    BoundedOutOfOrdernessWatermarks, SchemaTimestampAssigner, TimePeriodicWatermarks,
};

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
        if let Some(watermarks) = self.watermark_generator.take() {
            self.watermark_generator = Some(Box::new(TimePeriodicWatermarks::new(
                watermarks,
                process_period,
                event_period,
            )));
            self
        } else {
            panic!("no WatermarkGenerator for wrapper");
        }
    }

    pub fn wrap_idleness(mut self, idle_timeout: Duration) -> Self {
        if let Some(watermarks) = self.watermark_generator.take() {
            self.watermark_generator = Some(Box::new(WatermarksWithIdleness::new(
                watermarks,
                idle_timeout,
            )));
            self
        } else {
            panic!("no WatermarkGenerator for wrapper");
        }
    }

    pub fn for_schema_timestamp_assigner<T: ColumnLocateBuilder>(mut self, column: T) -> Self {
        self.timestamp_assigner = Some(Box::new(SchemaTimestampAssigner::new(column)));
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
