use std::time::Duration;

use rlink::core::backend::{CheckpointBackend, KeyedStateBackend};
use rlink::core::data_stream::CoStream;
use rlink::core::data_stream::{TConnectedStreams, TDataStream, TKeyedStream, TWindowedStream};
use rlink::core::env::{StreamApp, StreamExecutionEnvironment};
use rlink::core::properties::{ChannelBaseOn, Properties, SystemProperties};
use rlink::functions::flat_map::broadcast_flat_map::BroadcastFlagMapFunction;
use rlink::functions::flat_map::round_robin_flat_map::RoundRobinFlagMapFunction;
use rlink::functions::key_selector::SchemaKeySelector;
use rlink::functions::reduce::{pct_index, sum_index, SchemaReduceFunction};
use rlink::functions::sink::print::print_sink;
use rlink::functions::watermark::DefaultWatermarkStrategy;
use rlink::functions::window::SlidingEventTimeWindows;
use rlink_example_utils::buffer_gen::model;
use rlink_example_utils::config_input_format::ConfigInputFormat;
use rlink_example_utils::rand_input_format::RandInputFormat;

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
        let data_stream_left = env
            .register_source(RandInputFormat::new(), 2)
            .assign_timestamps_and_watermarks(
                DefaultWatermarkStrategy::new()
                    .for_bounded_out_of_orderness(Duration::from_secs(1))
                    .for_schema_timestamp_assigner(model::index::timestamp),
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
            .key_by(SchemaKeySelector::new(vec![model::index::name]))
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(60),
                None,
            ))
            .reduce(
                SchemaReduceFunction::new(vec![
                    sum_index(model::index::value),
                    pct_index(model::index::value, get_percentile_scale()),
                ]),
                2,
            )
            .add_sink(print_sink());
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
    }

    fn build_stream(&self, _properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let data_stream_left = env
            .register_source(RandInputFormat::new(), 3)
            .assign_timestamps_and_watermarks(
                DefaultWatermarkStrategy::new()
                    .for_bounded_out_of_orderness(Duration::from_secs(3))
                    .for_schema_timestamp_assigner(model::index::timestamp),
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
            .key_by(SchemaKeySelector::new(vec![model::index::name]))
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(10),
                Duration::from_secs(10),
                None,
            ))
            .reduce(
                SchemaReduceFunction::new(vec![
                    sum_index(model::index::value),
                    pct_index(model::index::value, get_percentile_scale()),
                ]),
                5,
            )
            .flat_map(OutputMapFunction::new(get_percentile_scale()))
            .connect(
                vec![CoStream::from(data_stream_right1)],
                MyCoProcessFunction {},
            )
            .add_sink(print_sink());
    }
}
