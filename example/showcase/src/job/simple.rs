use std::time::Duration;

use chrono::{DateTime, Utc};
use rlink::api::backend::KeyedStateBackend;
use rlink::api::data_stream::{TDataStream, TKeyedStream, TWindowedStream};
use rlink::api::element::Record;
use rlink::api::env::{StreamExecutionEnvironment, StreamJob};
use rlink::api::function::{
    Context, FilterFunction, InputFormat, InputSplit, InputSplitAssigner, InputSplitSource,
    MapFunction, OutputFormat,
};
use rlink::api::properties::{Properties, SystemProperties};
use rlink::api::watermark::BoundedOutOfOrdernessTimestampExtractor;
use rlink::api::window::{SlidingEventTimeWindows, Window};
use rlink::functions::column_base_function::key_selector::ColumnBaseKeySelector;
use rlink::functions::column_base_function::reduce::{sum_i64, ColumnBaseReduceFunction};
use rlink::functions::column_base_function::timestamp_assigner::ColumnBaseTimestampAssigner;
use rlink::functions::column_base_function::FunctionSchema;

use crate::buffer_gen::model;
use crate::buffer_gen::model::DATA_TYPE;

#[derive(Clone, Debug)]
pub struct MyStreamJob {}

impl StreamJob for MyStreamJob {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let key_selector = ColumnBaseKeySelector::new(vec![2], DATA_TYPE.to_vec());
        let reduce_function = ColumnBaseReduceFunction::new(vec![sum_i64(5)], DATA_TYPE.to_vec());

        // the schema after reduce
        let output_schema_types = {
            let mut key_types = key_selector.get_schema_types();
            let reduce_types = reduce_function.get_schema_types();
            key_types.extend_from_slice(reduce_types.as_slice());
            key_types
        };

        let data_stream = env.register_source(TestInputFormat::new(properties.clone()), 1);
        data_stream
            .map(MyMapFunction::new())
            .filter(MyFilterFunction::new())
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(1),
                ColumnBaseTimestampAssigner::new(0, DATA_TYPE.to_vec()),
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

impl InputSplitSource for TestInputFormat {
    fn create_input_splits(&self, min_num_splits: u32) -> Vec<InputSplit> {
        let mut input_splits = Vec::new();
        for i in 0..min_num_splits {
            input_splits.push(InputSplit::new(i, Properties::new()));
        }

        input_splits
    }

    fn get_input_split_assigner(&self, input_splits: Vec<InputSplit>) -> InputSplitAssigner {
        InputSplitAssigner::new(input_splits)
    }
}

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

    // rows.push(create_row("A", 13, "2222-03-11T12:02:20+0800"));
    // rows.push(create_row("A", 14, "2222-03-11T12:02:25+0800"));
    // rows.push(create_row("A", 15, "2222-03-11T12:02:35+0800"));
    // rows.push(create_row("B", 13, "2222-03-11T12:02:20+0800"));
    // rows.push(create_row("B", 14, "2222-03-11T12:02:25+0800"));
    // rows.push(create_row("B", 15, "2222-03-11T12:02:35+0800"));
    // rows.push(create_row("C", 13, "2222-03-11T12:02:20+0800"));
    // rows.push(create_row("C", 14, "2222-03-11T12:02:25+0800"));
    // rows.push(create_row("C", 15, "2222-03-11T12:02:35+0800"));

    rows
}

fn create_record(key: &str, value: i32, date_time: &str) -> Record {
    let timestamp = DateTime::parse_from_str(date_time, "%Y-%m-%dT%T%z")
        .map(|x| x.with_timezone(&Utc))
        .unwrap()
        .timestamp_millis() as u64;

    let model = model::Entity {
        timestamp,
        id: 0,
        name: key.to_string(),
        value1: value,
        value2: 1,
        value3: 2,
    };
    let mut record = Record::new();
    model.to_buffer(record.as_buffer()).unwrap();

    record
}

#[derive(Debug, Function)]
pub struct MyMapFunction {}

impl MyMapFunction {
    pub fn new() -> Self {
        MyMapFunction {}
    }
}

impl MapFunction for MyMapFunction {
    fn open(&mut self, _context: &Context) {}

    fn map(&mut self, t: &mut Record) -> Vec<Record> {
        vec![t.clone()]
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

        let mut reader = record.get_reader(self.date_type.as_slice());
        info!(
            "\tRecord output : 0:{}, 1:{}, 2:{}, 3:{}, window_max_ts:{}",
            reader.get_str(0).unwrap(),
            reader.get_i32(1).unwrap(),
            reader.get_u32(2).unwrap(),
            reader.get_i64(3).unwrap(),
            record.get_trigger_window().unwrap().max_timestamp(),
        );
    }

    fn close(&mut self) {}
}
