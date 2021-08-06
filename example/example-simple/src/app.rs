use std::time::Duration;

use rlink::core::backend::{CheckpointBackend, KeyedStateBackend};
use rlink::core::data_stream::{TDataStream, TKeyedStream, TWindowedStream};
use rlink::core::env::{StreamApp, StreamExecutionEnvironment};
use rlink::core::properties::{Properties, SystemProperties};
use rlink::core::runtime::ClusterDescriptor;
use rlink::functions::key_selector::SchemaKeySelector;
use rlink::functions::reduce::{sum_i64, SchemaReduceFunction};
use rlink::functions::sink::PrintOutputFormat;
use rlink::functions::watermark::DefaultWatermarkStrategy;
use rlink::functions::FunctionSchema;
use rlink_example_utils::bounded_input_format::VecInputFormat;
use rlink_example_utils::buffer_gen::model;
use rlink_example_utils::buffer_gen::model::FIELD_TYPE;

use crate::filter::MyFlatMapFunction;
use crate::mapper::MyFilterFunction;
use rlink::functions::window::SlidingEventTimeWindows;

#[derive(Clone, Debug)]
pub struct SimpleStreamApp {}

impl StreamApp for SimpleStreamApp {
    fn prepare_properties(&self, properties: &mut Properties) {
        // the `application_name` must be set in `prepare_properties`
        properties.set_application_name("rlink-simple");

        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
        properties.set_checkpoint_interval(Duration::from_secs(15));
        properties.set_checkpoint(CheckpointBackend::Memory);
    }

    fn build_stream(&self, _properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let key_selector = SchemaKeySelector::new(vec![model::index::name], &FIELD_TYPE);
        let reduce_function =
            SchemaReduceFunction::new(vec![sum_i64(model::index::value)], &FIELD_TYPE);

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
            .assign_timestamps_and_watermarks(
                DefaultWatermarkStrategy::new()
                    .for_bounded_out_of_orderness(Duration::from_secs(1))
                    .wrap_time_periodic(Duration::from_secs(10), Duration::from_secs(20))
                    .for_schema_timestamp_assigner(model::index::timestamp, &FIELD_TYPE),
            )
            .key_by(key_selector)
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(20),
                None,
            ))
            .reduce(reduce_function, 2)
            .add_sink(PrintOutputFormat::new(output_schema_types.as_slice()));
    }

    fn pre_worker_startup(&self, cluster_descriptor: &ClusterDescriptor) {
        println!("{}", cluster_descriptor.coordinator_manager.metrics_address);
    }
}
