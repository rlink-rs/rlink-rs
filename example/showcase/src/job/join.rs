use std::time::Duration;

use rlink::api::backend::KeyedStateBackend;
use rlink::api::data_stream::{TConnectedStreams, TDataStream, TKeyedStream, TWindowedStream};
use rlink::api::element::Record;
use rlink::api::env::{StreamExecutionEnvironment, StreamJob};
use rlink::api::function::{
    CoProcessFunction, Context, InputFormat, InputSplit, InputSplitAssigner, InputSplitSource,
};
use rlink::api::properties::{Properties, SystemProperties};
use rlink::api::watermark::BoundedOutOfOrdernessTimestampExtractor;
use rlink::api::window::SlidingEventTimeWindows;
use rlink::functions::column_base_function::key_selector::ColumnBaseKeySelector;
use rlink::functions::column_base_function::reduce::{sum_i64, ColumnBaseReduceFunction};
use rlink::functions::column_base_function::timestamp_assigner::ColumnBaseTimestampAssigner;
use rlink::functions::column_base_function::FunctionSchema;

use crate::buffer_gen::model::DATA_TYPE;
use crate::buffer_gen::{config, model};
use crate::job::simple::{MyFilterFunction, MyFlatMapFunction, MyOutputFormat, TestInputFormat};
use rlink::functions::broadcast_flat_map::BroadcastFlagMapFunction;

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

        let data_stream_left = env
            .register_source(TestInputFormat::new(properties.clone()), 3)
            .flat_map(MyFlatMapFunction::new())
            .filter(MyFilterFunction::new())
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(1),
                ColumnBaseTimestampAssigner::new(model::index::timestamp, DATA_TYPE.to_vec()),
            ));
        let data_stream_right = env
            .register_source(BroadcastInputFormat::new(properties.clone()), 1)
            .flat_map(BroadcastFlagMapFunction::new());

        data_stream_left
            .connect(vec![data_stream_right], MyCoProcessFunction {})
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
pub struct BroadcastInputFormat {
    data: Vec<Record>,

    properties: Properties,
}

impl BroadcastInputFormat {
    pub fn new(properties: Properties) -> Self {
        BroadcastInputFormat {
            data: Vec::new(),
            properties,
        }
    }
}

impl InputSplitSource for BroadcastInputFormat {
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

impl InputFormat for BroadcastInputFormat {
    fn open(&mut self, input_split: InputSplit, _context: &Context) {
        let partition_num = input_split.get_split_number();
        info!("open split number = {}", partition_num);

        let data = gen_row();

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

fn gen_row() -> Vec<Record> {
    let mut rows = Vec::new();

    rows.push(create_record("A1", "a1"));
    rows.push(create_record("A2", "a2"));
    rows.push(create_record("A3", "a3"));
    rows.push(create_record("A4", "a4"));
    rows.push(create_record("A5", "a6"));

    rows
}

fn create_record(key: &str, value: &str) -> Record {
    let model = config::Entity {
        field: key.to_string(),
        value: value.to_string(),
    };
    let mut record = Record::new();
    model.to_buffer(record.as_buffer()).unwrap();

    record
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
            info!("config field:{}, val:{}", conf.field, conf.value);
        }
        Box::new(vec![].into_iter())
    }

    fn close(&mut self) {}
}
