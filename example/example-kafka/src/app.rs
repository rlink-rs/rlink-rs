use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::Duration;

use rlink::channel::ChannelBaseOn;
use rlink::core::backend::KeyedStateBackend;
use rlink::core::data_stream::{TDataStream, TKeyedStream, TWindowedStream};
use rlink::core::data_types::Schema;
use rlink::core::env::{StreamApp, StreamExecutionEnvironment};
use rlink::core::properties::{FunctionProperties, Properties, SystemProperties};
use rlink::functions::key_selector::SchemaKeySelector;
use rlink::functions::reduce::{sum, SchemaReduceFunction};
use rlink::functions::sink::print::print_sink;
use rlink::functions::source::vec_input_format::vec_source;
use rlink::functions::watermark::DefaultWatermarkStrategy;
use rlink::functions::window::SlidingEventTimeWindows;
use rlink::utils::process::parse_arg;
use rlink_connector_kafka::sink::builder::KafkaOutputFormatBuilder;
use rlink_connector_kafka::source::builder::KafkaInputFormatBuilder;
use rlink_connector_kafka::source::offset_range::OffsetRange;
use rlink_connector_kafka::{
    state::PartitionOffset, BOOTSTRAP_SERVERS, GROUP_ID, KAFKA, OFFSET, TOPICS,
};
use rlink_example_utils::buffer_gen::model;
use rlink_example_utils::gen_record::gen_records;

use crate::input_mapper::InputMapperFunction;
use crate::output_mapper::OutputMapperFunction;

const KAFKA_FN: &'static str = "kafka_fn";

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

        let kafka_sink_properties = {
            let mut sink_properties = Properties::new();
            sink_properties.set_str(TOPICS, topic.as_str());

            let mut kafka_properties = Properties::new();
            kafka_properties.set_str(BOOTSTRAP_SERVERS, brokers.as_str());

            sink_properties.extend_sub_properties(KAFKA, kafka_properties);
            sink_properties
        };

        properties.extend_sink(KAFKA_FN, kafka_sink_properties);

        println!("{}", properties.to_lines_string());
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let sink = KafkaOutputFormatBuilder::try_from(properties.to_sink(KAFKA_FN))
            .unwrap()
            .build();

        env.register_source(vec_source(
            gen_records(),
            Schema::from(&model::FIELD_METADATA),
            3,
        ))
        .flat_map(OutputMapperFunction::new())
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

        let kafka_source_properties = {
            let mut source_properties = Properties::new();
            source_properties.set_u16("parallelism", 3);
            source_properties.set_str(TOPICS, topic.as_str());

            let mut kafka_properties = Properties::new();
            kafka_properties.set_str(BOOTSTRAP_SERVERS, brokers.as_str());
            kafka_properties.set_str(GROUP_ID, "rlink-test-consumer-group");
            source_properties.extend_sub_properties(KAFKA, kafka_properties);

            let offset_range = gen_kafka_offset_range(topic.as_str());
            let offset_properties = offset_range.into();
            source_properties.extend_sub_properties(OFFSET, offset_properties);

            source_properties
        };

        properties.extend_source(KAFKA_FN, kafka_source_properties);

        println!("{}", properties.to_lines_string());
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let source = KafkaInputFormatBuilder::try_from(properties.to_source(KAFKA_FN))
            .unwrap()
            .build(None);

        env.register_source(source).add_sink(print_sink());
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

        let kafka_source_properties = {
            let mut source_properties = Properties::new();
            source_properties.set_u16("parallelism", 3);
            source_properties.set_str(TOPICS, topic.as_str());

            let mut kafka_properties = Properties::new();
            kafka_properties.set_str(BOOTSTRAP_SERVERS, brokers.as_str());
            kafka_properties.set_str(GROUP_ID, "rlink-test-consumer-group");
            source_properties.extend_sub_properties(KAFKA, kafka_properties);

            let offset_range = gen_kafka_offset_range(topic.as_str());
            let offset_properties = offset_range.into();
            source_properties.extend_sub_properties(OFFSET, offset_properties);

            source_properties
        };

        properties.extend_source(KAFKA_FN, kafka_source_properties);
    }

    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let source = KafkaInputFormatBuilder::try_from(properties.to_source(KAFKA_FN))
            .unwrap()
            .build(None);

        env.register_source(source)
            .flat_map(InputMapperFunction::new())
            .assign_timestamps_and_watermarks(
                DefaultWatermarkStrategy::new()
                    .for_bounded_out_of_orderness(Duration::from_secs(1))
                    .for_schema_timestamp_assigner(model::index::timestamp),
            )
            .key_by(SchemaKeySelector::new(vec![model::index::name]))
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(20),
                None,
            ))
            .reduce(SchemaReduceFunction::new(vec![sum(model::index::value)], 2))
            .add_sink(print_sink());
    }
}

fn gen_kafka_offset_range(topic: &str) -> OffsetRange {
    let mut begin_offset = HashMap::new();
    begin_offset.insert(
        topic.to_string(),
        vec![
            PartitionOffset::new(0, 121),
            PartitionOffset::new(1, 71),
            PartitionOffset::new(2, 78),
        ],
    );
    let mut end_offset = HashMap::new();
    end_offset.insert(
        topic.to_string(),
        vec![
            PartitionOffset::new(0, 137),
            PartitionOffset::new(1, 84),
            PartitionOffset::new(2, 94),
        ],
    );

    OffsetRange::Direct {
        begin_offset,
        end_offset: Some(end_offset),
    }
}
