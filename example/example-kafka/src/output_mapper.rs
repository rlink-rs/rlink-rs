use rlink::core::element::Record;
use rlink::core::function::{Context, FlatMapFunction};
use rlink::utils::date_time::current_timestamp_millis;
use rlink_connector_kafka::build_kafka_record;
use rlink_example_utils::buffer_gen::model;

use crate::entry::SerDeEntity;

#[derive(Debug, Default, Function)]
pub struct OutputMapperFunction {
    topic: String,
}

impl OutputMapperFunction {
    pub fn new(topic: String) -> Self {
        OutputMapperFunction { topic }
    }
}

impl FlatMapFunction for OutputMapperFunction {
    fn open(&mut self, _context: &Context) -> rlink::core::Result<()> {
        Ok(())
    }

    fn flat_map(&mut self, mut record: Record) -> Box<dyn Iterator<Item = Record>> {
        let entry = model::Entity::parse(record.as_buffer()).unwrap();
        let entry = SerDeEntity {
            timestamp: entry.timestamp,
            name: entry.name.to_string(),
            value: entry.value,
        };

        let body = serde_json::to_string(&entry).unwrap();
        let key = format!("{}", uuid::Uuid::new_v4());
        let new_record = build_kafka_record(
            current_timestamp_millis() as i64,
            key.as_bytes(),
            body.as_bytes(),
            self.topic.as_str(),
            0,
            0,
        )
        .unwrap();
        Box::new(vec![new_record].into_iter())
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        Ok(())
    }
}
