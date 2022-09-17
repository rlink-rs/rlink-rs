use std::time::Duration;

use rlink::core;
use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{
    Context, InputFormat, InputSplit, InputSplitSource, SendableElementStream,
};
use rlink::utils::stream::IteratorStream;

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
        let field = format!("{}-{}", self.name, value);
        let model = config::Entity {
            field: field.as_str(),
            value: value,
        };
        let mut record = Record::new();
        model.to_buffer(record.as_buffer()).unwrap();

        record
    }
}

impl InputSplitSource for ConfigInputFormat {}

#[async_trait]
impl InputFormat for ConfigInputFormat {
    async fn open(&mut self, _input_split: InputSplit, _context: &Context) -> core::Result<()> {
        Ok(())
    }

    async fn element_stream(&mut self) -> SendableElementStream {
        let itr = IteratorStream::new(Box::new(ConfigIterator::new(self.gen_row())));
        Box::pin(itr)
    }

    async fn close(&mut self) -> core::Result<()> {
        Ok(())
    }

    fn daemon(&self) -> bool {
        true
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::from(&config::FIELD_METADATA)
    }

    fn parallelism(&self) -> u16 {
        1
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
