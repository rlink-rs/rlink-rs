use std::collections::HashMap;
use std::time::Duration;

use rlink::channel::ChannelBaseOn;
use rlink::core::backend::KeyedStateBackend;
use rlink::core::data_stream::{TDataStream, TKeyedStream, TWindowedStream};
use rlink::core::env::{StreamApp, StreamExecutionEnvironment};
use rlink::core::properties::{Properties, SystemProperties};
use rlink::functions::key_selector::SchemaKeySelector;
use rlink::functions::reduce::{sum_i64, SchemaReduceFunction};
use rlink::functions::source::vec_input_format::vec_source;
use rlink::functions::watermark::DefaultWatermarkStrategy;
use rlink::functions::window::SlidingEventTimeWindows;
use rlink::functions::FunctionSchema;
use rlink::utils::process::{parse_arg, parse_arg_to_u64};
use rlink_connector_kafka::{
    create_output_format, state::PartitionOffset, InputFormatBuilder, OffsetRange,
    BOOTSTRAP_SERVERS, GROUP_ID, KAFKA_DATA_TYPES,
};
use rlink_example_utils::buffer_gen::model;
use rlink_example_utils::gen_record::gen_fix_length_records;

use crate::buffer_gen::checkpoint_data::FIELD_TYPE;
use crate::input_mapper::InputMapperFunction;
use crate::output_mapper::OutputMapperFunction;
use rlink::functions::sink::print::print_sink;

#[derive(Clone, Debug)]
pub struct KafkaGenAppStream {}

impl KafkaGenAppStream {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {}
    }
}

impl StreamApp for KafkaGenAppStream {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_application_name("kafka-gen-example");

        properties.set_pub_sub_channel_size(1024 * 1000);
        properties.set_pub_sub_channel_base(ChannelBaseOn::Unbounded);
        properties.set_checkpoint_interval(Duration::from_secs(2 * 60));
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);

        let brokers = parse_arg("brokers").unwrap_or("localhost:9092".to_string());
        let topic = parse_arg("topic").unwrap_or("rlink-test".to_string());

        properties.set_str("kafka_broker_servers_sink", brokers.as_str());
        properties.set_str("kafka_topic_sink", topic.as_str());
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let kafka_broker_servers_sink = properties.get_string("kafka_broker_servers_sink").unwrap();
        let kafka_topic_sink = properties.get_string("kafka_topic_sink").unwrap();

        let mut conf_map = HashMap::new();
        conf_map.insert(BOOTSTRAP_SERVERS.to_string(), kafka_broker_servers_sink);

        let sink = create_output_format(conf_map, Some(kafka_topic_sink.clone()), None);

        env.register_source(vec_source(gen_fix_length_records()), 3)
            .flat_map(OutputMapperFunction::new(kafka_topic_sink))
            .add_sink(sink);
    }
}

#[derive(Clone, Debug)]
pub struct KafkaOffsetRangeAppStream {}

impl KafkaOffsetRangeAppStream {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {}
    }
}

impl StreamApp for KafkaOffsetRangeAppStream {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_application_name("kafka-offset-range-example");

        properties.set_pub_sub_channel_size(1024 * 1000);
        properties.set_pub_sub_channel_base(ChannelBaseOn::Unbounded);
        properties.set_checkpoint_interval(Duration::from_secs(2 * 60));
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);

        let brokers = parse_arg("brokers").unwrap_or("localhost:9092".to_string());
        let topic = parse_arg("topic").unwrap_or("rlink-test".to_string());

        properties.set_str("kafka_broker_servers_source", brokers.as_str());
        properties.set_str("kafka_topic_source", topic.as_str());
        properties.set_str("kafka_group_id", "rlink-test-consumer-group");
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let kafka_group_id = properties.get_string("kafka_group_id").unwrap();
        let kafka_broker_servers_source = properties
            .get_string("kafka_broker_servers_source")
            .unwrap();
        let kafka_topic_source = properties.get_string("kafka_topic_source").unwrap();

        let kafka_input_format = {
            let mut conf_map = HashMap::new();
            conf_map.insert(BOOTSTRAP_SERVERS.to_string(), kafka_broker_servers_source);
            conf_map.insert(GROUP_ID.to_string(), kafka_group_id);

            InputFormatBuilder::new(conf_map, vec![kafka_topic_source.clone()], None)
                .offset_range(gen_kafka_offset_range(kafka_topic_source.as_str()))
                .build()
        };

        env.register_source(kafka_input_format, 3)
            .add_sink(print_sink(&KAFKA_DATA_TYPES));
    }
}

#[derive(Clone, Debug)]
pub struct KafkaReplayAppStream {}

impl KafkaReplayAppStream {
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {}
    }
}

impl StreamApp for KafkaReplayAppStream {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_application_name("kafka-replay-example");

        // properties.set_checkpoint(CheckpointBackend::MySql {
        //     endpoint: String::from("mysql://root@loaclhost:3306/rlink"),
        //     table: Some("rlink_ck".to_string()),
        // });
        properties.set_pub_sub_channel_size(1024 * 1000);
        properties.set_pub_sub_channel_base(ChannelBaseOn::Unbounded);
        properties.set_checkpoint_interval(Duration::from_secs(2 * 60));
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);

        let brokers = parse_arg("brokers").unwrap_or("localhost:9092".to_string());
        let topic = parse_arg("topic").unwrap_or("rlink-test".to_string());
        let source_parallelism = parse_arg_to_u64("source_parallelism").unwrap_or(3);

        properties.set_str("kafka_group_id", "rlink-test-consumer-group");
        properties.set_str("kafka_broker_servers_source", brokers.as_str());
        properties.set_str("kafka_topic_source", topic.as_str());
        properties.set_u64("source_parallelism", source_parallelism);
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let kafka_group_id = properties.get_string("kafka_group_id").unwrap();
        let kafka_broker_servers_source = properties
            .get_string("kafka_broker_servers_source")
            .unwrap();
        let kafka_topic_source = properties.get_string("kafka_topic_source").unwrap();
        let source_parallelism = properties.get_u64("source_parallelism").unwrap() as u16;

        let kafka_input_format = {
            let mut conf_map = HashMap::new();
            conf_map.insert(BOOTSTRAP_SERVERS.to_string(), kafka_broker_servers_source);
            conf_map.insert(GROUP_ID.to_string(), kafka_group_id);

            InputFormatBuilder::new(conf_map, vec![kafka_topic_source.clone()], None)
                .offset_range(gen_kafka_offset_range(kafka_topic_source.as_str()))
                .build()
        };

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

        env.register_source(kafka_input_format, source_parallelism)
            .flat_map(InputMapperFunction::new())
            .assign_timestamps_and_watermarks(
                DefaultWatermarkStrategy::new()
                    .for_bounded_out_of_orderness(Duration::from_secs(1))
                    .for_schema_timestamp_assigner(model::index::timestamp, &FIELD_TYPE),
            )
            .key_by(key_selector)
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(20),
                None,
            ))
            .reduce(reduce_function, 2)
            .add_sink(print_sink(output_schema_types.as_slice()));
    }
}

fn gen_kafka_offset_range(topic: &str) -> OffsetRange {
    let mut begin_offset = HashMap::new();
    begin_offset.insert(
        topic.to_string(),
        vec![
            PartitionOffset::new(0, 72),
            PartitionOffset::new(1, 32),
            PartitionOffset::new(2, 31),
        ],
    );
    let mut end_offset = HashMap::new();
    end_offset.insert(
        topic.to_string(),
        vec![
            PartitionOffset::new(0, 87),
            PartitionOffset::new(1, 48),
            PartitionOffset::new(2, 45),
        ],
    );

    OffsetRange::Direct {
        begin_offset,
        end_offset: Some(end_offset),
    }
}
