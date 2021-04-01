use std::time::Duration;

use serbuffer::types;

use crate::api::checkpoint::CheckpointFunction;
use crate::api::element::Record;
use crate::api::function::{Context, NamedFunction, OutputFormat};
use crate::api::runtime::TaskId;
use crate::api::window::TWindow;
use crate::utils::date_time::fmt_date_time;

pub struct PrintOutputFormat {
    task_id: TaskId,
    field_types: Vec<u8>,
}

impl PrintOutputFormat {
    pub fn new(field_types: &[u8]) -> Self {
        PrintOutputFormat {
            task_id: TaskId::default(),
            field_types: field_types.to_vec(),
        }
    }
}

impl OutputFormat for PrintOutputFormat {
    fn open(&mut self, context: &Context) -> crate::api::Result<()> {
        self.task_id = context.task_id;

        Ok(())
    }

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

        let window_str = record
            .trigger_window()
            .map(|window| {
                let min_timestamp = window.min_timestamp();
                let max_timestamp = window.max_timestamp();
                format!(
                    "[{}, {}]",
                    fmt_date_time(Duration::from_millis(min_timestamp), "%T"),
                    fmt_date_time(Duration::from_millis(max_timestamp), "%T")
                )
            })
            .unwrap_or_default();

        println!(
            "task_number: {}, window: {}, row: [{}]",
            self.task_id.task_number,
            window_str,
            field_str_vec.join(","),
        );
    }

    fn close(&mut self) -> crate::api::Result<()> {
        Ok(())
    }
}

impl NamedFunction for PrintOutputFormat {
    fn name(&self) -> &str {
        "PrintOutputFormat"
    }
}

impl CheckpointFunction for PrintOutputFormat {}
