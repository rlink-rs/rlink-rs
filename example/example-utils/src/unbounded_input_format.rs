use std::time::Duration;

use rand::Rng;
use rlink::api;
use rlink::api::element::Record;
use rlink::api::function::{Context, InputFormat, InputSplit, InputSplitSource};
use rlink::utils::date_time::current_timestamp_millis;

use crate::buffer_gen::model;

#[derive(Debug, Function)]
pub struct RandInputFormat {}

impl RandInputFormat {
    pub fn new() -> Self {
        RandInputFormat {}
    }
}

impl InputSplitSource for RandInputFormat {}

impl InputFormat for RandInputFormat {
    fn open(&mut self, _input_split: InputSplit, _context: &Context) -> api::Result<()> {
        Ok(())
    }

    fn record_iter(&mut self) -> Box<dyn Iterator<Item = Record> + Send> {
        Box::new(RandIterator::new())
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
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
        std::thread::sleep(Duration::from_millis(1));

        let mut thread_rng = rand::thread_rng();
        let v = thread_rng.gen_range(0i32, 100i32) as i64;

        let model = model::Entity {
            timestamp: current_timestamp_millis(),
            name: format!("name-{}", v),
            value: v,
        };

        let mut record = Record::new();
        model.to_buffer(record.as_buffer()).unwrap();

        Some(record)
    }
}
