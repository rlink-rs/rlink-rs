#[macro_use]
extern crate rlink_derive;
#[macro_use]
extern crate log;

extern crate anyhow;

mod app;
mod buffer_gen;
mod filter;
mod kafka_input_mapper;
mod writer;

use std::{collections::HashMap, str::FromStr};

use rlink::utils::process::parse_arg;
use rlink_connector_kafka::state::PartitionOffset;

fn main() {
    let brokers = parse_arg("brokers")
        .unwrap_or("localhost:9092".to_string())
        .to_string();
    let topic = parse_arg("topic")
        .unwrap_or("my-topic".to_string())
        .to_string();
    let kafka_partitions = {
        let n = parse_arg("source_parallelism").unwrap_or("3".to_string());
        u16::from_str(n.as_str()).unwrap()
    };

    let mut begin_offsets = HashMap::new();
    begin_offsets.insert(
        topic.clone(),
        vec![
            PartitionOffset::new(0, 24253),
            PartitionOffset::new(1, 24644),
            PartitionOffset::new(2, 24701),
        ],
    );
    let mut end_offsets = HashMap::new();
    end_offsets.insert(
        topic.clone(),
        vec![
            PartitionOffset::new(0, 24553),
            PartitionOffset::new(1, 24944),
            PartitionOffset::new(2, 25001),
        ],
    );

    rlink::core::env::execute(app::ReplayApp::new(
        brokers,
        topic,
        kafka_partitions,
        begin_offsets,
        end_offsets,
    ));
}
