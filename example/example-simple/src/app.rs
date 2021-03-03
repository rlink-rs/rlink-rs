use std::time::Duration;

use rlink::api::backend::{CheckpointBackend, KeyedStateBackend};
use rlink::api::data_stream::{TDataStream, TKeyedStream, TWindowedStream};
use rlink::api::env::{StreamApp, StreamExecutionEnvironment};
use rlink::api::properties::{Properties, SystemProperties};
use rlink::api::watermark::BoundedOutOfOrdernessTimestampExtractor;
use rlink::api::window::SlidingEventTimeWindows;
use rlink::functions::schema_base::key_selector::SchemaBaseKeySelector;
use rlink::functions::schema_base::print_output_format::PrintOutputFormat;
use rlink::functions::schema_base::reduce::{sum_i64, SchemaBaseReduceFunction};
use rlink::functions::schema_base::timestamp_assigner::SchemaBaseTimestampAssigner;
use rlink::functions::schema_base::FunctionSchema;
use rlink_example_utils::bounded_input_format::VecInputFormat;
use rlink_example_utils::buffer_gen::model;
use rlink_example_utils::buffer_gen::model::FIELD_TYPE;

use crate::filter::MyFlatMapFunction;
use crate::mapper::MyFilterFunction;

#[derive(Clone, Debug)]
pub struct SimpleStreamApp {}

impl StreamApp for SimpleStreamApp {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
        properties.set_checkpoint_internal(Duration::from_secs(15));
        properties.set_checkpoint(CheckpointBackend::Memory);
    }

    fn build_stream(&self, _properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let key_selector = SchemaBaseKeySelector::new(vec![model::index::name], &FIELD_TYPE);
        let reduce_function =
            SchemaBaseReduceFunction::new(vec![sum_i64(model::index::value)], &FIELD_TYPE);

        // the schema after reduce
        let output_schema_types = {
            let mut key_types = key_selector.schema_types();
            let reduce_types = reduce_function.schema_types();
            key_types.extend_from_slice(reduce_types.as_slice());
            key_types
        };

        env.register_source(VecInputFormat::new(), 3)
            .flat_map(MyFlatMapFunction::new())
            .filter(MyFilterFunction::new())
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(1),
                SchemaBaseTimestampAssigner::new(model::index::timestamp, &FIELD_TYPE),
            ))
            .key_by(key_selector)
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(20),
                None,
            ))
            .reduce(reduce_function, 2)
            .add_sink(PrintOutputFormat::new(output_schema_types.as_slice()));
    }
}
