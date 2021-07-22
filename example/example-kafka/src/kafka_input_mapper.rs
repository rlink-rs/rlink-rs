use std::borrow::BorrowMut;
use std::error::Error;
use std::iter::Iterator;

use rlink::core;
use rlink::core::{
    element::Record,
    function::{Context, FlatMapFunction},
};
use rlink_connector_kafka::KafkaRecord;
use serde_json::Value;

use crate::buffer_gen::checkpoint_data::FieldWriter;

#[derive(Debug, Default, Function)]
pub struct KafkaInputMapperFunction {
    err_counter: u64,
    topic: String,
}

impl KafkaInputMapperFunction {
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
        let mut records: Vec<Record> = Vec::new();
        let kafka_record = KafkaRecord::new(record.borrow_mut());

        let payload = match kafka_record.get_kafka_payload() {
            Ok(payload) => payload,
            _ => {
                warn!("no payload!");
                return Box::new(records.into_iter());
            }
        };

        let line = match String::from_utf8(payload.to_vec()) {
            Ok(line) => line,
            _ => {
                warn!("get line failed");
                return Box::new(records.into_iter());
            }
        };

        let record_new = match parse_data(line.as_str()) {
            Ok(record_new) => record_new,
            Err(_e) => {
                self.err_counter += 1;
                // error!("parse data error,{},{:?}", _e, line);
                return Box::new(records.into_iter());
            }
        };

        records.push(record_new);

        Box::new(records.into_iter())
    }
    fn close(&mut self) -> core::Result<()> {
        Ok(())
    }
}

fn parse_data(line: &str) -> Result<Record, Box<dyn Error>> {
    let json: Value = serde_json::from_str(line)?;
    let json_map = json.as_object().ok_or("log is not json")?;
    let mut record_new = Record::new();
    let mut writer = FieldWriter::new(record_new.as_buffer());
    writer.set_ts(json_map.get("ts").unwrap().as_u64().unwrap())?;
    writer.set_app(json_map.get("app").unwrap().as_str().unwrap())?;
    writer.set_count(json_map.get("count").unwrap().as_i64().unwrap())?;

    Ok(record_new)
}
