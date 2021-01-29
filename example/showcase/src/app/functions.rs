use std::time::Duration;

use chrono::{DateTime, Utc};
use rand::Rng;
use rlink::api;
use rlink::api::element::Record;
use rlink::api::function::{
    CoProcessFunction, Context, FilterFunction, FlatMapFunction, InputFormat, InputSplit,
    InputSplitSource,
};
use rlink::api::properties::Properties;
use rlink::utils::date_time::current_timestamp_millis;

use crate::buffer_gen::{config, model};

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
            name: v.to_string(),
            value: v,
        };
        let mut record = Record::new();
        model.to_buffer(record.as_buffer()).unwrap();

        Some(record)
    }
}

#[derive(Debug, Function)]
pub struct TestInputFormat {
    data: Vec<Record>,

    properties: Properties,
}

impl TestInputFormat {
    pub fn new(properties: Properties) -> Self {
        TestInputFormat {
            data: Vec::new(),
            properties,
        }
    }

    fn gen_row(&self) -> Vec<Record> {
        let mut rows = Vec::new();

        rows.push(self.create_record("A-key-0", 1, "2020-03-11T12:01:00+0800"));
        rows.push(self.create_record("A-key-0", 2, "2020-03-11T12:01:05+0800"));
        rows.push(self.create_record("A-key-0", 3, "2020-03-11T12:01:15+0800"));
        rows.push(self.create_record("B-key-0", 1, "2020-03-11T12:01:00+0800"));
        rows.push(self.create_record("B-key-0", 2, "2020-03-11T12:01:05+0800"));
        rows.push(self.create_record("B-key-0", 3, "2020-03-11T12:01:15+0800"));
        rows.push(self.create_record("C-key-0", 1, "2020-03-11T12:01:00+0800"));
        rows.push(self.create_record("C-key-0", 2, "2020-03-11T12:01:05+0800"));
        rows.push(self.create_record("C-key-0", 3, "2020-03-11T12:01:15+0800"));

        rows.push(self.create_record("A-key-0", 4, "2020-03-11T12:01:20+0800"));
        rows.push(self.create_record("A-key-0", 5, "2020-03-11T12:01:25+0800"));
        rows.push(self.create_record("A-key-0", 6, "2020-03-11T12:01:35+0800"));
        rows.push(self.create_record("B-key-0", 4, "2020-03-11T12:01:20+0800"));
        rows.push(self.create_record("B-key-0", 5, "2020-03-11T12:01:25+0800"));
        rows.push(self.create_record("B-key-0", 6, "2020-03-11T12:01:35+0800"));
        rows.push(self.create_record("C-key-0", 4, "2020-03-11T12:01:20+0800"));
        rows.push(self.create_record("C-key-0", 5, "2020-03-11T12:01:25+0800"));
        rows.push(self.create_record("C-key-0", 6, "2020-03-11T12:01:35+0800"));

        rows.push(self.create_record("A-key-0", 7, "2020-03-11T12:01:40+0800"));
        rows.push(self.create_record("A-key-0", 8, "2020-03-11T12:01:45+0800"));
        rows.push(self.create_record("A-key-0", 9, "2020-03-11T12:01:55+0800"));
        rows.push(self.create_record("B-key-0", 7, "2020-03-11T12:01:40+0800"));
        rows.push(self.create_record("B-key-0", 8, "2020-03-11T12:01:45+0800"));
        rows.push(self.create_record("B-key-0", 9, "2020-03-11T12:01:55+0800"));
        rows.push(self.create_record("C-key-0", 7, "2020-03-11T12:01:40+0800"));
        rows.push(self.create_record("C-key-0", 8, "2020-03-11T12:01:45+0800"));
        rows.push(self.create_record("C-key-0", 9, "2020-03-11T12:01:55+0800"));

        rows.push(self.create_record("A-key-0", 10, "2020-03-11T12:02:00+0800"));
        rows.push(self.create_record("A-key-0", 11, "2020-03-11T12:02:05+0800"));
        rows.push(self.create_record("A-key-0", 12, "2020-03-11T12:02:15+0800"));
        rows.push(self.create_record("B-key-0", 10, "2020-03-11T12:02:00+0800"));
        rows.push(self.create_record("B-key-0", 11, "2020-03-11T12:02:05+0800"));
        rows.push(self.create_record("B-key-0", 12, "2020-03-11T12:02:15+0800"));
        rows.push(self.create_record("C-key-0", 10, "2020-03-11T12:02:00+0800"));
        rows.push(self.create_record("C-key-0", 11, "2020-03-11T12:02:05+0800"));
        rows.push(self.create_record("C-key-0", 12, "2020-03-11T12:02:15+0800"));

        rows.push(self.create_record("A-key-0", 13, "2020-03-11T12:02:20+0800"));
        rows.push(self.create_record("A-key-0", 14, "2020-03-11T12:02:25+0800"));
        rows.push(self.create_record("A-key-0", 15, "2020-03-11T12:02:35+0800"));
        rows.push(self.create_record("B-key-0", 13, "2020-03-11T12:02:20+0800"));
        rows.push(self.create_record("B-key-0", 14, "2020-03-11T12:02:25+0800"));
        rows.push(self.create_record("B-key-0", 15, "2020-03-11T12:02:35+0800"));
        rows.push(self.create_record("C-key-0", 13, "2020-03-11T12:02:20+0800"));
        rows.push(self.create_record("C-key-0", 14, "2020-03-11T12:02:25+0800"));
        rows.push(self.create_record("C-key-0", 15, "2020-03-11T12:02:35+0800"));

        rows
    }

    fn create_record(&self, key: &str, value: i32, date_time: &str) -> Record {
        let timestamp = DateTime::parse_from_str(date_time, "%Y-%m-%dT%T%z")
            .map(|x| x.with_timezone(&Utc))
            .unwrap()
            .timestamp_millis() as u64;

        let model = model::Entity {
            timestamp,
            name: key.to_string(),
            value: value as i64,
        };
        let mut record = Record::new();
        model.to_buffer(record.as_buffer()).unwrap();

        record
    }
}

impl InputSplitSource for TestInputFormat {}

impl InputFormat for TestInputFormat {
    fn open(&mut self, _input_split: InputSplit, context: &Context) -> api::Result<()> {
        let task_number = context.task_id.task_number() as usize;
        let num_tasks = context.task_id.num_tasks() as usize;

        let data = self.gen_row();
        (0..data.len())
            .filter(|i| i % num_tasks == task_number)
            .for_each(|i| {
                self.data.push(data[i].clone());
            });

        Ok(())
    }

    fn record_iter(&mut self) -> Box<dyn Iterator<Item = Record> + Send> {
        Box::new(self.data.clone().into_iter())
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
}

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
    fn open(&mut self, input_split: InputSplit, _context: &Context) -> api::Result<()> {
        let partition_num = input_split.get_split_number();
        info!("open split number = {}", partition_num);

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

#[derive(Debug, Function)]
pub struct MyCoProcessFunction {}

impl CoProcessFunction for MyCoProcessFunction {
    fn open(&mut self, _context: &Context) -> api::Result<()> {
        Ok(())
    }

    fn process_left(&self, record: Record) -> Box<dyn Iterator<Item = Record>> {
        Box::new(vec![record].into_iter())
    }

    fn process_right(
        &self,
        stream_seq: usize,
        mut record: Record,
    ) -> Box<dyn Iterator<Item = Record>> {
        let conf = config::Entity::parse(record.as_buffer()).unwrap();
        info!(
            "Right Stream: {}, config [field:{}, val:{}]",
            stream_seq, conf.field, conf.value
        );

        Box::new(vec![].into_iter())
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Function)]
pub struct MyFlatMapFunction {}

impl MyFlatMapFunction {
    pub fn new() -> Self {
        MyFlatMapFunction {}
    }
}

impl FlatMapFunction for MyFlatMapFunction {
    fn open(&mut self, _context: &Context) -> api::Result<()> {
        Ok(())
    }

    fn flat_map(&mut self, record: Record) -> Box<dyn Iterator<Item = Record>> {
        Box::new(vec![record].into_iter())
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Function)]
pub struct MyFilterFunction {}

impl MyFilterFunction {
    pub fn new() -> Self {
        MyFilterFunction {}
    }
}

impl FilterFunction for MyFilterFunction {
    fn open(&mut self, _context: &Context) -> api::Result<()> {
        Ok(())
    }

    fn filter(&self, _t: &mut Record) -> bool {
        true
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
}
