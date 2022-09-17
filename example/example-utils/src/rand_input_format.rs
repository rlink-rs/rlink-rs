use std::time::Duration;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rlink::core;
use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{
    Context, InputFormat, InputSplit, InputSplitSource, SendableElementStream,
};
use rlink::utils::date_time::current_timestamp_millis;
use rlink::utils::stream::IteratorStream;

use crate::buffer_gen::model;

#[derive(Debug, Function)]
pub struct RandInputFormat {
    parallelism: u16,
}

impl RandInputFormat {
    pub fn new(parallelism: u16) -> Self {
        RandInputFormat { parallelism }
    }
}

impl InputSplitSource for RandInputFormat {}

#[async_trait]
impl InputFormat for RandInputFormat {
    async fn open(&mut self, _input_split: InputSplit, _context: &Context) -> core::Result<()> {
        Ok(())
    }

    async fn element_stream(&mut self) -> SendableElementStream {
        let itr = IteratorStream::new(Box::new(RandIterator::new()));
        Box::pin(itr)
    }

    async fn close(&mut self) -> core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::from(&model::FIELD_METADATA)
    }

    fn parallelism(&self) -> u16 {
        self.parallelism
    }
}

struct RandIterator {}

impl RandIterator {
    pub fn new() -> Self {
        RandIterator {}
    }
}

impl Iterator for RandIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        std::thread::sleep(Duration::from_millis(10));

        let mut rng: StdRng = SeedableRng::from_entropy();
        let v = rng.gen_range(0i32..100i32) as i64;

        let name = format!("name-{}", v);
        let model = model::Entity {
            timestamp: current_timestamp_millis(),
            name: name.as_str(),
            value: v,
        };

        let mut record = Record::new();
        model.to_buffer(record.as_buffer()).unwrap();

        Some(record)
    }
}
