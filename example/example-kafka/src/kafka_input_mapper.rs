use std::error::Error;
use std::iter::Iterator;

use rlink::core;
use rlink::core::element::FnSchema;
use rlink::core::{
    element::Record,
    function::{Context, FlatMapFunction},
};
use serde_json::Value;

use crate::buffer_gen::checkpoint_data;
use rlink_connector_kafka::buffer_gen::kafka_message;

#[derive(Debug, Default, Function)]
#[allow(dead_code)]
pub struct KafkaInputMapperFunction {
    err_counter: u64,
    topic: String,
}

impl KafkaInputMapperFunction {
    #[allow(dead_code)]
    pub fn new(topic: String) -> Self {
        KafkaInputMapperFunction {
            err_counter: 0,
            topic,
        }
    }
}

impl FlatMapFunction for KafkaInputMapperFunction {
    fn open(&mut self, _context: &Context) -> core::Result<()> {
        Ok(())
    }

    fn flat_map(&mut self, mut record: Record) -> Box<dyn Iterator<Item = Record>> {
        let kafka_message::Entity { payload, .. } =
            kafka_message::Entity::parse(record.as_buffer()).unwrap();

        let line = String::from_utf8(payload.to_vec()).unwrap();
        let record_new = parse_data(line.as_str()).unwrap();

        Box::new(vec![record_new].into_iter())
    }
    fn close(&mut self) -> core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::from(&checkpoint_data::FIELD_METADATA)
    }
}

fn parse_data(line: &str) -> Result<Record, Box<dyn Error>> {
    let json: Value = serde_json::from_str(line)?;
    let json_map = json.as_object().ok_or("log is not json")?;

    let mut record_new = Record::new();
    let mut writer = checkpoint_data::FieldWriter::new(record_new.as_buffer());

    writer.set_ts(json_map.get("ts").unwrap().as_u64().unwrap())?;
    writer.set_app(json_map.get("app").unwrap().as_str().unwrap())?;
    writer.set_count(json_map.get("count").unwrap().as_i64().unwrap())?;

    Ok(record_new)
}
