use std::time::Duration;

use rlink::api;
use rlink::api::element::Record;
use rlink::api::function::{Context, InputFormat, InputSplit, InputSplitSource};

use crate::buffer_gen::config;

#[derive(Debug, Function)]
pub struct ConfigInputFormat {
    name: &'static str,
}

impl ConfigInputFormat {
    pub fn new(name: &'static str) -> Self {
        ConfigInputFormat { name }
    }

    fn gen_row(&self) -> Vec<Record> {
        let mut rows = Vec::new();

        rows.push(self.create_record("0"));
        rows.push(self.create_record("1"));
        rows.push(self.create_record("2"));
        rows.push(self.create_record("3"));
        rows.push(self.create_record("4"));

        rows
    }

    fn create_record(&self, value: &str) -> Record {
        let model = config::Entity {
            field: format!("{}-{}", self.name, value),
            value: value.to_string(),
        };
        let mut record = Record::new();
        model.to_buffer(record.as_buffer()).unwrap();

        record
    }
}

impl InputSplitSource for ConfigInputFormat {}

impl InputFormat for ConfigInputFormat {
    fn open(&mut self, _input_split: InputSplit, _context: &Context) -> api::Result<()> {
        Ok(())
    }

    fn record_iter(&mut self) -> Box<dyn Iterator<Item = Record> + Send> {
        Box::new(ConfigIterator::new(self.gen_row()))
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
}

struct ConfigIterator {
    conf: std::vec::IntoIter<Record>,
}

impl ConfigIterator {
    pub fn new(conf: Vec<Record>) -> Self {
        ConfigIterator {
            conf: conf.into_iter(),
        }
    }
}

impl Iterator for ConfigIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        match self.conf.next() {
            Some(record) => Some(record),
            None => loop {
                std::thread::sleep(Duration::from_secs(60));
            },
        }
    }
}
