use rlink::core::element::{Element, FnSchema, Record};
use rlink::core::function::{Context, FlatMapFunction, SendableElementStream};
use rlink::utils::stream::MemoryStream;
use rlink_connector_kafka::buffer_gen::kafka_message;
use rlink_example_utils::buffer_gen::model;

use crate::entry::SerDeEntity;

#[derive(Debug, Default, Function)]
pub struct InputMapperFunction {}

impl InputMapperFunction {
    pub fn new() -> Self {
        InputMapperFunction {}
    }
}

#[async_trait]
impl FlatMapFunction for InputMapperFunction {
    async fn open(&mut self, _context: &Context) -> rlink::core::Result<()> {
        Ok(())
    }

    async fn flat_map_element(&mut self, element: Element) -> SendableElementStream {
        let mut record = element.into_record();
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

        Box::pin(MemoryStream::new(vec![new_record]))
    }

    async fn close(&mut self) -> rlink::core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::from(&model::FIELD_METADATA)
    }
}
