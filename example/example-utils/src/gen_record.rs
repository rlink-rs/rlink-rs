use chrono::DateTime;
use chrono::Utc;
use rlink::core::element::Record;

use crate::buffer_gen::model;

pub fn gen_fix_length_records() -> Vec<Record> {
    let mut rows = Vec::new();

    rows.push(create_record("A-key-0", 1, "2020-03-11T12:01:00+0800"));
    rows.push(create_record("A-key-0", 2, "2020-03-11T12:01:05+0800"));
    rows.push(create_record("A-key-0", 3, "2020-03-11T12:01:15+0800"));
    rows.push(create_record("B-key-0", 1, "2020-03-11T12:01:00+0800"));
    rows.push(create_record("B-key-0", 2, "2020-03-11T12:01:05+0800"));
    rows.push(create_record("B-key-0", 3, "2020-03-11T12:01:15+0800"));
    rows.push(create_record("C-key-0", 1, "2020-03-11T12:01:00+0800"));
    rows.push(create_record("C-key-0", 2, "2020-03-11T12:01:05+0800"));
    rows.push(create_record("C-key-0", 3, "2020-03-11T12:01:15+0800"));

    rows.push(create_record("A-key-0", 4, "2020-03-11T12:01:20+0800"));
    rows.push(create_record("A-key-0", 5, "2020-03-11T12:01:25+0800"));
    rows.push(create_record("A-key-0", 6, "2020-03-11T12:01:35+0800"));
    rows.push(create_record("B-key-0", 4, "2020-03-11T12:01:20+0800"));
    rows.push(create_record("B-key-0", 5, "2020-03-11T12:01:25+0800"));
    rows.push(create_record("B-key-0", 6, "2020-03-11T12:01:35+0800"));
    rows.push(create_record("C-key-0", 4, "2020-03-11T12:01:20+0800"));
    rows.push(create_record("C-key-0", 5, "2020-03-11T12:01:25+0800"));
    rows.push(create_record("C-key-0", 6, "2020-03-11T12:01:35+0800"));

    rows.push(create_record("A-key-0", 7, "2020-03-11T12:01:40+0800"));
    rows.push(create_record("A-key-0", 8, "2020-03-11T12:01:45+0800"));
    rows.push(create_record("A-key-0", 9, "2020-03-11T12:01:55+0800"));
    rows.push(create_record("B-key-0", 7, "2020-03-11T12:01:40+0800"));
    rows.push(create_record("B-key-0", 8, "2020-03-11T12:01:45+0800"));
    rows.push(create_record("B-key-0", 9, "2020-03-11T12:01:55+0800"));
    rows.push(create_record("C-key-0", 7, "2020-03-11T12:01:40+0800"));
    rows.push(create_record("C-key-0", 8, "2020-03-11T12:01:45+0800"));
    rows.push(create_record("C-key-0", 9, "2020-03-11T12:01:55+0800"));

    rows.push(create_record("A-key-0", 10, "2020-03-11T12:02:00+0800"));
    rows.push(create_record("A-key-0", 11, "2020-03-11T12:02:05+0800"));
    rows.push(create_record("A-key-0", 12, "2020-03-11T12:02:15+0800"));
    rows.push(create_record("B-key-0", 10, "2020-03-11T12:02:00+0800"));
    rows.push(create_record("B-key-0", 11, "2020-03-11T12:02:05+0800"));
    rows.push(create_record("B-key-0", 12, "2020-03-11T12:02:15+0800"));
    rows.push(create_record("C-key-0", 10, "2020-03-11T12:02:00+0800"));
    rows.push(create_record("C-key-0", 11, "2020-03-11T12:02:05+0800"));
    rows.push(create_record("C-key-0", 12, "2020-03-11T12:02:15+0800"));

    rows.push(create_record("A-key-0", 13, "2020-03-11T12:02:20+0800"));
    rows.push(create_record("A-key-0", 14, "2020-03-11T12:02:25+0800"));
    rows.push(create_record("A-key-0", 15, "2020-03-11T12:02:35+0800"));
    rows.push(create_record("B-key-0", 13, "2020-03-11T12:02:20+0800"));
    rows.push(create_record("B-key-0", 14, "2020-03-11T12:02:25+0800"));
    rows.push(create_record("B-key-0", 15, "2020-03-11T12:02:35+0800"));
    rows.push(create_record("C-key-0", 13, "2020-03-11T12:02:20+0800"));
    rows.push(create_record("C-key-0", 14, "2020-03-11T12:02:25+0800"));
    rows.push(create_record("C-key-0", 15, "2020-03-11T12:02:35+0800"));

    rows
}

fn create_record(key: &str, value: i32, date_time: &str) -> Record {
    let timestamp = DateTime::parse_from_str(date_time, "%Y-%m-%dT%T%z")
        .map(|x| x.with_timezone(&Utc))
        .unwrap()
        .timestamp_millis() as u64;

    let model = model::Entity {
        timestamp,
        name: key,
        value: value as i64,
    };
    let mut record = Record::new();
    model.to_buffer(record.as_buffer()).unwrap();

    record
}
