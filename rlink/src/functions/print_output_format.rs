use crate::api::element::Record;
use crate::api::function::{Context, Function, OutputFormat};
use crate::api::window::Window;
use crate::utils::date_time::{fmt_date_time, FMT_DATE_TIME_1};
use serbuffer::types;
use std::time::Duration;

pub struct PrintOutputFormat {
    field_types: Vec<u8>,
}

impl PrintOutputFormat {
    pub fn new(field_types: Vec<u8>) -> Self {
        PrintOutputFormat { field_types }
    }
}

impl OutputFormat for PrintOutputFormat {
    fn open(&mut self, _context: &Context) {}

    fn write_record(&mut self, mut record: Record) {
        let mut reader = record.as_buffer().as_reader(self.field_types.as_slice());
        let mut field_str_vec = Vec::new();
        for i in 0..self.field_types.len() {
            let field_type = self.field_types[i];
            let field_str = match field_type {
                types::BOOL => reader.get_bool(i).unwrap().to_string(),
                types::I8 => reader.get_i8(i).unwrap().to_string(),
                types::U8 => reader.get_u8(i).unwrap().to_string(),
                types::I16 => reader.get_i16(i).unwrap().to_string(),
                types::U16 => reader.get_i16(i).unwrap().to_string(),
                types::I32 => reader.get_u16(i).unwrap().to_string(),
                types::U32 => reader.get_i32(i).unwrap().to_string(),
                types::I64 => reader.get_i64(i).unwrap().to_string(),
                types::U64 => reader.get_u64(i).unwrap().to_string(),
                types::F32 => reader.get_f32(i).unwrap().to_string(),
                types::F64 => reader.get_f64(i).unwrap().to_string(),
                types::BYTES => match reader.get_str(i) {
                    Ok(s) => s,
                    Err(_e) => format!("{:?}", reader.get_bytes(i).unwrap()),
                },
                // types::STRING => reader.get_bool(i).unwrap().to_string(),
                _ => panic!("unknown type"),
            };

            field_str_vec.push(format!("{}:{}", i, field_str));
        }

        let window_str = record.get_trigger_window().map(|window| {
            let min_timestamp = window.min_timestamp();
            let max_timestamp = window.max_timestamp();
            format!(
                "start:{}({}), end:{}({})",
                min_timestamp,
                fmt_date_time(Duration::from_millis(min_timestamp), FMT_DATE_TIME_1),
                max_timestamp,
                fmt_date_time(Duration::from_millis(max_timestamp), FMT_DATE_TIME_1)
            )
        });

        println!(
            "row: [{}], window: {:?}",
            field_str_vec.join(","),
            window_str
        );
    }

    fn close(&mut self) {}
}

impl Function for PrintOutputFormat {
    fn get_name(&self) -> &str {
        "PrintOutputFormat"
    }
}
