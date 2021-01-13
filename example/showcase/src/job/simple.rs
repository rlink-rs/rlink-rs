use std::time::Duration;

use chrono::{DateTime, Utc};
use rlink::api::backend::KeyedStateBackend;
use rlink::api::data_stream::{TDataStream, TKeyedStream, TWindowedStream};
use rlink::api::element::Record;
use rlink::api::env::{StreamExecutionEnvironment, StreamJob};
use rlink::api::function::{
    Context, FilterFunction, FlatMapFunction, InputFormat, InputSplit, InputSplitSource,
    OutputFormat,
};
use rlink::api::properties::{Properties, SystemProperties};
use rlink::api::watermark::BoundedOutOfOrdernessTimestampExtractor;
use rlink::api::window::{SlidingEventTimeWindows, Window};
use rlink::functions::column_base_function::key_selector::ColumnBaseKeySelector;
use rlink::functions::column_base_function::reduce::{sum_i64, ColumnBaseReduceFunction};
use rlink::functions::column_base_function::timestamp_assigner::ColumnBaseTimestampAssigner;
use rlink::functions::column_base_function::FunctionSchema;
use rlink::utils::date_time::{fmt_date_time, FMT_DATE_TIME_1};

use crate::buffer_gen::model;
use crate::buffer_gen::model::DATA_TYPE;

#[derive(Clone, Debug)]
pub struct MyStreamJob {}

impl StreamJob for MyStreamJob {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let key_selector = ColumnBaseKeySelector::new(vec![model::index::name], DATA_TYPE.to_vec());
        let reduce_function =
            ColumnBaseReduceFunction::new(vec![sum_i64(model::index::value3)], DATA_TYPE.to_vec());

        // the schema after reduce
        let output_schema_types = {
            let mut key_types = key_selector.get_schema_types();
            let reduce_types = reduce_function.get_schema_types();
            key_types.extend_from_slice(reduce_types.as_slice());
            key_types
        };

        let data_stream = env.register_source(TestInputFormat::new(properties.clone()), 1);
        data_stream
            .flat_map(MyFlatMapFunction::new())
            .filter(MyFilterFunction::new())
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(1),
                ColumnBaseTimestampAssigner::new(model::index::timestamp, DATA_TYPE.to_vec()),
            ))
            .key_by(key_selector)
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(20),
                None,
            ))
            .reduce(reduce_function, 2)
            .add_sink(MyOutputFormat::new(output_schema_types));
    }
}

#[derive(Debug, Function)]
pub struct TestInputFormat {
    data: Vec<Record>,
    step_index: usize,

    properties: Properties,
}

impl TestInputFormat {
    pub fn new(properties: Properties) -> Self {
        TestInputFormat {
            data: Vec::new(),
            step_index: 0,
            properties,
        }
    }
}

impl InputSplitSource for TestInputFormat {}

impl InputFormat for TestInputFormat {
    fn open(&mut self, input_split: InputSplit, _context: &Context) {
        let partition_num = input_split.get_split_number();
        info!("open split number = {}", partition_num);

        let data = gen_row();

        self.data.extend(data);
        self.step_index = 0;
    }

    fn reached_end(&self) -> bool {
        self.step_index == self.data.len()
    }

    fn next_record(&mut self) -> Option<Record> {
        if self.step_index < self.data.len() {
            let record = self.data.get(self.step_index).unwrap().clone();
            self.step_index += 1;
            Some(record)
        } else {
            None
        }
    }

    fn close(&mut self) {}
}

fn gen_row() -> Vec<Record> {
    let mut rows = Vec::new();

    rows.push(create_record("A-key-0", 1, "2020-03-11T12:01:00+0800"));
    rows.push(create_record("A-key-0", 2, "2020-03-11T12:01:05+0800"));
    rows.push(create_record("A-key-0", 3, "2020-03-11T12:01:15+0800"));
    rows.push(create_record("B-key-0", 1, "2020-03-11T12:01:00+0800"));
    rows.push(create_record("B-key-0", 2, "2020-03-11T12:01:05+0800"));
    rows.push(create_record("B-key-0", 3, "2020-03-11T12:01:15+0800"));
    rows.push(create_record("C-key-0", 1, "2020-03-11T12:01:00+0800"));
    rows.push(create_record("C-key-0", 2, "2020-03-11T12:01:05+0800"));
    rows.push(create_record("C-key-0", 3, "2020-03-11T12:01:15+0800"));

    rows.push(create_record("A-key-0", 4, "2020-03-11T12:01:20+0800"));
    rows.push(create_record("A-key-0", 5, "2020-03-11T12:01:25+0800"));
    rows.push(create_record("A-key-0", 6, "2020-03-11T12:01:35+0800"));
    rows.push(create_record("B-key-0", 4, "2020-03-11T12:01:20+0800"));
    rows.push(create_record("B-key-0", 5, "2020-03-11T12:01:25+0800"));
    rows.push(create_record("B-key-0", 6, "2020-03-11T12:01:35+0800"));
    rows.push(create_record("C-key-0", 4, "2020-03-11T12:01:20+0800"));
    rows.push(create_record("C-key-0", 5, "2020-03-11T12:01:25+0800"));
    rows.push(create_record("C-key-0", 6, "2020-03-11T12:01:35+0800"));

    rows.push(create_record("A-key-0", 7, "2020-03-11T12:01:40+0800"));
    rows.push(create_record("A-key-0", 8, "2020-03-11T12:01:45+0800"));
    rows.push(create_record("A-key-0", 9, "2020-03-11T12:01:55+0800"));
    rows.push(create_record("B-key-0", 7, "2020-03-11T12:01:40+0800"));
    rows.push(create_record("B-key-0", 8, "2020-03-11T12:01:45+0800"));
    rows.push(create_record("B-key-0", 9, "2020-03-11T12:01:55+0800"));
    rows.push(create_record("C-key-0", 7, "2020-03-11T12:01:40+0800"));
    rows.push(create_record("C-key-0", 8, "2020-03-11T12:01:45+0800"));
    rows.push(create_record("C-key-0", 9, "2020-03-11T12:01:55+0800"));

    rows.push(create_record("A-key-0", 10, "2020-03-11T12:02:00+0800"));
    rows.push(create_record("A-key-0", 11, "2020-03-11T12:02:05+0800"));
    rows.push(create_record("A-key-0", 12, "2020-03-11T12:02:15+0800"));
    rows.push(create_record("B-key-0", 10, "2020-03-11T12:02:00+0800"));
    rows.push(create_record("B-key-0", 11, "2020-03-11T12:02:05+0800"));
    rows.push(create_record("B-key-0", 12, "2020-03-11T12:02:15+0800"));
    rows.push(create_record("C-key-0", 10, "2020-03-11T12:02:00+0800"));
    rows.push(create_record("C-key-0", 11, "2020-03-11T12:02:05+0800"));
    rows.push(create_record("C-key-0", 12, "2020-03-11T12:02:15+0800"));

    rows.push(create_record("A-key-0", 13, "2020-03-11T12:02:20+0800"));
    rows.push(create_record("A-key-0", 14, "2020-03-11T12:02:25+0800"));
    rows.push(create_record("A-key-0", 15, "2020-03-11T12:02:35+0800"));
    rows.push(create_record("B-key-0", 13, "2020-03-11T12:02:20+0800"));
    rows.push(create_record("B-key-0", 14, "2020-03-11T12:02:25+0800"));
    rows.push(create_record("B-key-0", 15, "2020-03-11T12:02:35+0800"));
    rows.push(create_record("C-key-0", 13, "2020-03-11T12:02:20+0800"));
    rows.push(create_record("C-key-0", 14, "2020-03-11T12:02:25+0800"));
    rows.push(create_record("C-key-0", 15, "2020-03-11T12:02:35+0800"));

    rows
}

fn create_record(key: &str, value: i32, date_time: &str) -> Record {
    let timestamp = DateTime::parse_from_str(date_time, "%Y-%m-%dT%T%z")
        .map(|x| x.with_timezone(&Utc))
        .unwrap()
        .timestamp_millis() as u64;

    let model = model::Entity {
        timestamp,
        id: 1,
        name: key.to_string(),
        value1: value,
        value2: 1,
        value3: value as i64,
    };
    let mut record = Record::new();
    model.to_buffer(record.as_buffer()).unwrap();

    record
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

    fn filter(&self, t: &mut Record) -> bool {
        // do not deserialize all fields
        // let mut reader = t.get_reader(&DATA_TYPE);
        // reader.get_u32(1).unwrap() > 0

        // deserialize all fields to Entity
        let m = model::Entity::parse(t.as_buffer()).unwrap();
        m.id > 0
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
