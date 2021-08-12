use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{Context, FlatMapFunction};
use rlink_example_utils::buffer_gen::model;

use crate::entry::SerDeEntity;
use rlink_connector_kafka::buffer_gen::kafka_message;

#[derive(Debug, Default, Function)]
pub struct InputMapperFunction {}

impl InputMapperFunction {
    pub fn new() -> Self {
        InputMapperFunction {}
    }
}

impl FlatMapFunction for InputMapperFunction {
    fn open(&mut self, _context: &Context) -> rlink::core::Result<()> {
        Ok(())
    }

    fn flat_map(&mut self, mut record: Record) -> Box<dyn Iterator<Item = Record>> {
        let kafka_message::Entity { payload, .. } =
            kafka_message::Entity::parse(record.as_buffer()).unwrap();

        let entry: SerDeEntity = serde_json::from_slice(payload).unwrap();
        let entry = model::Entity {
            timestamp: entry.timestamp,
            name: entry.name.as_str(),
            value: entry.value,
        };

        let mut new_record = Record::new();
        entry.to_buffer(new_record.as_buffer()).unwrap();

        Box::new(vec![new_record].into_iter())
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::from(&model::FIELD_METADATA)
    }
}
