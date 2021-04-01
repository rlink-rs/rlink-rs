use std::time::Duration;

use rlink::api::backend::KeyedStateBackend;
use rlink::api::data_stream::CoStream;
use rlink::api::data_stream::{TConnectedStreams, TDataStream, TKeyedStream, TWindowedStream};
use rlink::api::env::{StreamApp, StreamExecutionEnvironment};
use rlink::api::properties::{ChannelBaseOn, Properties, SystemProperties};
use rlink::api::watermark::BoundedOutOfOrdernessTimestampExtractor;
use rlink::api::window::SlidingEventTimeWindows;
use rlink::functions::broadcast_flat_map::BroadcastFlagMapFunction;
use rlink::functions::round_robin_flat_map::RoundRobinFlagMapFunction;
use rlink::functions::schema_base::key_selector::SchemaBaseKeySelector;
use rlink::functions::schema_base::print_output_format::PrintOutputFormat;
use rlink::functions::schema_base::reduce::{sum_i64, SchemaBaseReduceFunction};
use rlink::functions::schema_base::timestamp_assigner::SchemaBaseTimestampAssigner;
use rlink::functions::schema_base::FunctionSchema;
use rlink_example_utils::buffer_gen::model;
use rlink_example_utils::buffer_gen::model::FIELD_TYPE;
use rlink_example_utils::config_input_format::ConfigInputFormat;
use rlink_example_utils::unbounded_input_format::RandInputFormat;

use crate::co_connect::MyCoProcessFunction;

#[derive(Clone, Debug)]
pub struct ConnectStreamApp0 {}

impl StreamApp for ConnectStreamApp0 {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
        properties.set_pub_sub_channel_size(100000);
        properties.set_pub_sub_channel_base(ChannelBaseOn::UnBounded);
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

        let data_stream_left = env
            .register_source(RandInputFormat::new(), 2)
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(1),
                SchemaBaseTimestampAssigner::new(model::index::timestamp, &FIELD_TYPE),
            ));
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
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
        properties.set_pub_sub_channel_size(100000);
        properties.set_pub_sub_channel_base(ChannelBaseOn::UnBounded);
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

        let data_stream_left = env
            .register_source(RandInputFormat::new(), 2)
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(1),
                SchemaBaseTimestampAssigner::new(model::index::timestamp, &FIELD_TYPE),
            ));
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
            .reduce(reduce_function, 2)
            .connect(
                vec![CoStream::from(data_stream_right1)],
                MyCoProcessFunction {},
            )
            .add_sink(PrintOutputFormat::new(output_schema_types.as_slice()));
    }
}
