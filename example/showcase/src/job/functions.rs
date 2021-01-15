use crate::buffer_gen::{config, model};
use chrono::{DateTime, Utc};
use rlink::api::element::Record;
use rlink::api::function::{
    CoProcessFunction, Context, FilterFunction, FlatMapFunction, InputFormat, InputSplit,
    InputSplitSource, OutputFormat,
};
use rlink::api::properties::Properties;
use rlink::api::window::Window;
use rlink::utils::date_time::{fmt_date_time, FMT_DATE_TIME_1};
use std::time::Duration;

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
    fn open(&mut self, _input_split: InputSplit, context: &Context) {
        let task_number = context.task_id.task_number() as usize;
        let num_tasks = context.task_id.num_tasks() as usize;

        let data = self.gen_row();
        (0..data.len())
            .filter(|i| i % num_tasks == task_number)
            .for_each(|i| {
                self.data.push(data[i].clone());
            });
    }

    fn reached_end(&self) -> bool {
        self.data.len() == 0
    }

    fn next_record(&mut self) -> Option<Record> {
        if self.data.len() > 0 {
            Some(self.data.remove(0))
        } else {
            None
        }
    }

    fn close(&mut self) {}
}

#[derive(Debug, Function)]
pub struct ConfigInputFormat {
    name: &'static str,
    data: Vec<Record>,
}

impl ConfigInputFormat {
    pub fn new(name: &'static str) -> Self {
        ConfigInputFormat {
            name,
            data: Vec::new(),
        }
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
    fn open(&mut self, input_split: InputSplit, _context: &Context) {
        let partition_num = input_split.get_split_number();
        info!("open split number = {}", partition_num);

        let data = self.gen_row();

        self.data.extend(data);
    }

    fn reached_end(&self) -> bool {
        false
    }

    fn next_record(&mut self) -> Option<Record> {
        if self.data.len() > 0 {
            Some(self.data.remove(0))
        } else {
            None
        }
    }

    fn close(&mut self) {}
}

#[derive(Debug, Function)]
pub struct MyCoProcessFunction {}

impl CoProcessFunction for MyCoProcessFunction {
    fn open(&mut self, _context: &Context) {}

    fn process_left(&self, record: Record) -> Box<dyn Iterator<Item = Record>> {
        Box::new(vec![record].into_iter())
    }

    fn process_right(
        &self,
        stream_seq: usize,
        mut record: Record,
    ) -> Box<dyn Iterator<Item = Record>> {
        if stream_seq == 0 {
            let conf = config::Entity::parse(record.as_buffer()).unwrap();
            info!("Broadcast config field:{}, val:{}", conf.field, conf.value);
        } else if stream_seq == 1 {
            let conf = config::Entity::parse(record.as_buffer()).unwrap();
            info!("RoundRobin config field:{}, val:{}", conf.field, conf.value);
        }
        Box::new(vec![].into_iter())
    }

    fn close(&mut self) {}
}

#[derive(Debug, Function)]
pub struct MyFlatMapFunction {}

impl MyFlatMapFunction {
    pub fn new() -> Self {
        MyFlatMapFunction {}
    }
}

impl FlatMapFunction for MyFlatMapFunction {
    fn open(&mut self, _context: &Context) {}

    fn flat_map(&mut self, record: Record) -> Box<dyn Iterator<Item = Record>> {
        Box::new(vec![record].into_iter())
    }

    fn close(&mut self) {}
}

#[derive(Debug, Function)]
pub struct MyFilterFunction {}

impl MyFilterFunction {
    pub fn new() -> Self {
        MyFilterFunction {}
    }
}

impl FilterFunction for MyFilterFunction {
    fn open(&mut self, _context: &Context) {}

    fn filter(&self, _t: &mut Record) -> bool {
        true
    }

    fn close(&mut self) {}
}

#[derive(Debug, Function)]
pub struct MyOutputFormat {
    date_type: Vec<u8>,
}

impl MyOutputFormat {
    pub fn new(date_type: Vec<u8>) -> Self {
        MyOutputFormat { date_type }
    }
}

impl OutputFormat for MyOutputFormat {
    fn open(&mut self, _context: &Context) {}

    fn write_record(&mut self, mut record: Record) {
        // info!("{}:write_record", self.get_name());

        let window_time = {
            let min_timestamp = record.get_trigger_window().unwrap().min_timestamp();
            fmt_date_time(Duration::from_millis(min_timestamp), FMT_DATE_TIME_1)
        };

        let mut reader = record.get_reader(self.date_type.as_slice());
        info!(
            "Record output : 0:{}, 1:{}, window_min_ts:{}",
            reader.get_str(0).unwrap(),
            reader.get_i64(1).unwrap(),
            window_time,
        );
    }

    fn close(&mut self) {}
}
