use rlink::channel::ChannelBaseOn;
use rlink::core::backend::{CheckpointBackend, KeyedStateBackend};
use rlink::core::data_stream::{TDataStream, TKeyedStream, TWindowedStream};
use rlink::core::env::{StreamApp, StreamExecutionEnvironment};
use rlink::core::properties::{Properties, SystemProperties};
use rlink::core::watermark::BoundedOutOfOrdernessTimestampExtractor;
use rlink::core::window::SlidingEventTimeWindows;
use rlink::functions::schema_base::key_selector::SchemaBaseKeySelector;
use rlink::functions::schema_base::print_output_format::PrintOutputFormat;
use rlink::functions::schema_base::reduce::{sum_i64, SchemaBaseReduceFunction};
use rlink::functions::schema_base::timestamp_assigner::SchemaBaseTimestampAssigner;
use rlink::functions::schema_base::FunctionSchema;

use rlink_connector_kafka::state::PartitionOffsets;
// use rlink_connector_kafka::create_input_format;
use rlink_connector_kafka::{
    state::PartitionOffset, InputFormatBuilder, BOOTSTRAP_SERVERS, GROUP_ID,
};
use std::hash::Hash;
use std::time::Duration;

use crate::buffer_gen::checkpoint_data::{self, FIELD_TYPE};
use crate::filter::RangeFilterFunction;

use crate::kafka_input_mapper::KafkaInputMapperFunction;

use std::collections::HashMap;


#[derive(Clone, Debug)]
pub struct ReplayApp {
    brokers:String,
    topic: String,
    source_parallelism: u16,
    begin_offset: HashMap<String, Vec<PartitionOffset>>,
    end_offset: HashMap<String, Vec<PartitionOffset>>,
}

impl ReplayApp {
    pub fn new(
        brokers:String,
        topic: String,
        source_parallelism: u16,
        
        begin_offset: HashMap<String, Vec<PartitionOffset>>,
        end_offset: HashMap<String, Vec<PartitionOffset>>,
    ) -> Self {
        Self {
            brokers,
            topic,
            source_parallelism,
            begin_offset,
            end_offset
        }
    }
}

impl StreamApp for ReplayApp {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_checkpoint(CheckpointBackend::MySql {
            endpoint: String::from("mysql://root@loaclhost:3306/rlink"),
            table: Some("rlink_ck".to_string()),
        });
        properties.set_pub_sub_channel_size(1024 * 1000);
        properties.set_pub_sub_channel_base(ChannelBaseOn::Unbounded);
        properties.set_checkpoint_internal(Duration::from_secs(2 * 60));
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);

        properties.set_str("kafka_group_id", "my-consumer-group");
        properties.set_str("kafka_broker_servers_source", self.brokers.as_str());
        properties.set_str("kafka_topic_source", self.topic.as_str());
    }
    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment) {
        let kafka_broker_servers_source = properties
            .get_string("kafka_broker_servers_source")
            .unwrap();

        let kafka_topic_source = properties.get_string("kafka_topic_source").unwrap();

        let kafka_input_format = {
            let mut conf_map = HashMap::new();
            conf_map.insert(BOOTSTRAP_SERVERS.to_string(), kafka_broker_servers_source);
            conf_map.insert(
                GROUP_ID.to_string(),
                properties.get_string("kafka_group_id").unwrap(),
            );

            InputFormatBuilder::new(conf_map, vec![kafka_topic_source.clone()], None)
                .begin_offset(self.begin_offset.clone())
                .end_offset(self.end_offset.clone())
                .build()
        };

        let key_selector =
            SchemaBaseKeySelector::new(vec![checkpoint_data::index::app], &FIELD_TYPE);

        let reduce_function = SchemaBaseReduceFunction::new(
            vec![sum_i64(checkpoint_data::index::count)],
            &FIELD_TYPE,
        );

        let output_schema_types = {
            let mut key_types = key_selector.schema_types();
            let reduce_types = reduce_function.schema_types();
            key_types.extend_from_slice(reduce_types.as_slice());
            key_types
        };


        env.register_source(kafka_input_format, self.source_parallelism)
            .flat_map(KafkaInputMapperFunction::new(kafka_topic_source.clone()))
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(10),
                SchemaBaseTimestampAssigner::new(checkpoint_data::index::ts, &FIELD_TYPE),
            ))
            .key_by(key_selector)
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(60),
                None,
            ))
            .reduce(reduce_function, 1)
            .add_sink(PrintOutputFormat::new(output_schema_types.as_slice()));
    }
}
