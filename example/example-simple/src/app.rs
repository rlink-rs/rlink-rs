use std::time::Duration;

use rlink::core::backend::{CheckpointBackend, KeyedStateBackend};
use rlink::core::data_stream::{TDataStream, TKeyedStream, TWindowedStream};
use rlink::core::env::{StreamApp, StreamExecutionEnvironment};
use rlink::core::properties::{Properties, SystemProperties};
use rlink::core::runtime::ClusterDescriptor;
use rlink::functions::key_selector::SchemaKeySelector;
use rlink::functions::reduce::{count, max, min, sum, SchemaReduceFunction};
use rlink::functions::sink::print_sink;
use rlink::functions::watermark::DefaultWatermarkStrategy;
use rlink::functions::window::SlidingEventTimeWindows;
use rlink_example_utils::buffer_gen::model;
use rlink_example_utils::rand_input_format::RandInputFormat;

use crate::filter::MyFlatMapFunction;
use crate::mapper::MyFilterFunction;

#[derive(Clone, Debug)]
pub struct SimpleStreamApp {}

#[async_trait]
impl StreamApp for SimpleStreamApp {
    async fn prepare_properties(&self, properties: &mut Properties) {
        // the `application_name` must be set in `prepare_properties`
        properties.set_application_name("rlink-simple");

        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
        properties.set_checkpoint_interval(Duration::from_secs(15));
        properties.set_checkpoint(CheckpointBackend::Memory);
    }

    fn build_stream(&self, _properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let input_format = RandInputFormat::new(1);
        // let input_format = vec_source(
        //     gen_records(),
        //     Schema::from(&model::FIELD_METADATA),
        //     3,
        // );
        env.register_source(input_format)
            .flat_map(MyFlatMapFunction::new())
            .filter(MyFilterFunction::new())
            .assign_timestamps_and_watermarks(
                DefaultWatermarkStrategy::new()
                    .for_bounded_out_of_orderness(Duration::from_secs(1))
                    .wrap_time_periodic(Duration::from_secs(10), Duration::from_secs(20))
                    .for_schema_timestamp_assigner(model::index::timestamp),
            )
            .key_by(SchemaKeySelector::new(vec![model::index::name]))
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(20),
                None,
            ))
            .reduce(SchemaReduceFunction::new(
                vec![
                    sum(model::index::value),
                    max(model::index::value),
                    min(model::index::value),
                    count(),
                ],
                2,
            ))
            .add_sink(print_sink());
    }

    async fn pre_worker_startup(&self, cluster_descriptor: &ClusterDescriptor) {
        println!("{}", cluster_descriptor.coordinator_manager.web_address);
    }
}
