use std::time::Duration;

use rlink::core::backend::{CheckpointBackend, KeyedStateBackend};
use rlink::core::data_stream::CoStream;
use rlink::core::data_stream::{TConnectedStreams, TDataStream, TKeyedStream, TWindowedStream};
use rlink::core::env::{StreamApp, StreamExecutionEnvironment};
use rlink::core::properties::{ChannelBaseOn, Properties, SystemProperties};
use rlink::functions::flat_map::broadcast_flat_map::BroadcastFlagMapFunction;
use rlink::functions::flat_map::round_robin_flat_map::RoundRobinFlagMapFunction;
use rlink::functions::key_selector::SchemaKeySelector;
use rlink::functions::reduce::{pct_u64, sum_i64, SchemaReduceFunction};
use rlink::functions::sink::PrintOutputFormat;
use rlink::functions::watermark::DefaultWatermarkStrategy;
use rlink::functions::window::SlidingEventTimeWindows;
use rlink::functions::FunctionSchema;
use rlink_example_utils::buffer_gen::model::FIELD_TYPE;
use rlink_example_utils::buffer_gen::{model, output};
use rlink_example_utils::config_input_format::ConfigInputFormat;
use rlink_example_utils::unbounded_input_format::RandInputFormat;

use crate::co_connect::MyCoProcessFunction;
use crate::map_output::OutputMapFunction;
use crate::percentile::get_percentile_scale;

#[derive(Clone, Debug)]
pub struct ConnectStreamApp0 {}

impl StreamApp for ConnectStreamApp0 {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
        properties.set_pub_sub_channel_size(100000);
        properties.set_pub_sub_channel_base(ChannelBaseOn::Unbounded);
    }

    fn build_stream(&self, _properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let key_selector = SchemaKeySelector::new(vec![model::index::name], &FIELD_TYPE);
        let reduce_function = SchemaReduceFunction::new(
            vec![
                sum_i64(model::index::value),
                pct_u64(model::index::value, get_percentile_scale()),
            ],
            &FIELD_TYPE,
        );

        // the schema after reduce
        let output_schema_types = {
            let mut key_types = key_selector.schema_types();
            let reduce_types = reduce_function.schema_types();
            key_types.extend_from_slice(reduce_types.as_slice());
            key_types
        };

        let data_stream_left = env
            .register_source(RandInputFormat::new(), 2)
            .assign_timestamps_and_watermarks(
                DefaultWatermarkStrategy::new()
                    .for_bounded_out_of_orderness(Duration::from_secs(1))
                    .for_schema_timestamp_assigner(model::index::timestamp, &FIELD_TYPE),
            );
        let data_stream_right = env
            .register_source(ConfigInputFormat::new("Broadcast"), 1)
            .flat_map(BroadcastFlagMapFunction::new());

        let data_stream_right1 = env
            .register_source(ConfigInputFormat::new("RoundRobin"), 1)
            .flat_map(RoundRobinFlagMapFunction::new());

        data_stream_left
            .connect(
                vec![
                    CoStream::from(data_stream_right),
                    CoStream::from(data_stream_right1),
                ],
                MyCoProcessFunction {},
            )
            .key_by(key_selector)
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(60),
                None,
            ))
            .reduce(reduce_function, 2)
            .add_sink(PrintOutputFormat::new(output_schema_types.as_slice()));
    }
}

#[derive(Clone, Debug)]
pub struct ConnectStreamApp1 {}

impl StreamApp for ConnectStreamApp1 {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_application_name("rlink-connect");
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
        properties.set_pub_sub_channel_size(100000);
        properties.set_pub_sub_channel_base(ChannelBaseOn::Unbounded);

        properties.set_checkpoint(CheckpointBackend::Memory);
        properties.set_checkpoint_interval(Duration::from_secs(15));
        properties.set_checkpoint_ttl(Duration::from_secs(60 * 60 * 24));

        properties.set_speed_backpressure_interval(Duration::from_secs(10));
        properties.set_speed_backpressure_max_times(2);
        properties.set_speed_backpressure_pause_time(Duration::from_secs(5));
    }

    fn build_stream(&self, _properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let key_selector = SchemaKeySelector::new(vec![model::index::name], &FIELD_TYPE);
        let reduce_function = SchemaReduceFunction::new(
            vec![
                sum_i64(model::index::value),
                pct_u64(model::index::value, get_percentile_scale()),
            ],
            &FIELD_TYPE,
        );

        // the schema after reduce
        let output_schema_types = {
            let mut key_types = key_selector.schema_types();
            let reduce_types = reduce_function.schema_types();
            key_types.extend_from_slice(reduce_types.as_slice());
            key_types
        };

        let data_stream_left = env
            .register_source(RandInputFormat::new(), 3)
            .assign_timestamps_and_watermarks(
                DefaultWatermarkStrategy::new()
                    .for_bounded_out_of_orderness(Duration::from_secs(1))
                    .for_schema_timestamp_assigner(model::index::timestamp, &FIELD_TYPE),
            );
        let data_stream_right = env
            .register_source(ConfigInputFormat::new("Broadcast"), 1)
            .flat_map(BroadcastFlagMapFunction::new());

        let data_stream_right1 = env
            .register_source(ConfigInputFormat::new("RoundRobin"), 1)
            .flat_map(RoundRobinFlagMapFunction::new());

        data_stream_left
            .connect(
                vec![CoStream::from(data_stream_right)],
                MyCoProcessFunction {},
            )
            .key_by(key_selector)
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(60),
                None,
            ))
            .reduce(reduce_function, 5)
            .flat_map(OutputMapFunction::new(
                output_schema_types,
                get_percentile_scale(),
            ))
            .connect(
                vec![CoStream::from(data_stream_right1)],
                MyCoProcessFunction {},
            )
            .add_sink(PrintOutputFormat::new(&output::FIELD_TYPE));
    }
}
