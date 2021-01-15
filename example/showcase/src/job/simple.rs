use std::time::Duration;

use rlink::api::backend::KeyedStateBackend;
use rlink::api::data_stream::{TDataStream, TKeyedStream, TWindowedStream};
use rlink::api::env::{StreamExecutionEnvironment, StreamJob};
use rlink::api::properties::{Properties, SystemProperties};
use rlink::api::watermark::BoundedOutOfOrdernessTimestampExtractor;
use rlink::api::window::SlidingEventTimeWindows;
use rlink::functions::column_base_function::key_selector::ColumnBaseKeySelector;
use rlink::functions::column_base_function::reduce::{sum_i64, ColumnBaseReduceFunction};
use rlink::functions::column_base_function::timestamp_assigner::ColumnBaseTimestampAssigner;
use rlink::functions::column_base_function::FunctionSchema;

use crate::buffer_gen::model;
use crate::buffer_gen::model::FIELD_TYPE;
use crate::job::functions::{MyFilterFunction, MyFlatMapFunction, MyOutputFormat, TestInputFormat};

#[derive(Clone, Debug)]
pub struct MyStreamJob {}

impl StreamJob for MyStreamJob {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let key_selector =
            ColumnBaseKeySelector::new(vec![model::index::name], FIELD_TYPE.to_vec());
        let reduce_function =
            ColumnBaseReduceFunction::new(vec![sum_i64(model::index::value)], FIELD_TYPE.to_vec());

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
                ColumnBaseTimestampAssigner::new(model::index::timestamp, FIELD_TYPE.to_vec()),
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
