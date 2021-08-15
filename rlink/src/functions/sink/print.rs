use std::time::Duration;

use crate::core::checkpoint::CheckpointFunction;
use crate::core::data_types::{DataType, Schema};
use crate::core::element::{FnSchema, Record};
use crate::core::function::{Context, NamedFunction, OutputFormat};
use crate::core::runtime::TaskId;
use crate::core::window::TWindow;
use crate::utils::date_time::fmt_date_time;

pub fn print_sink() -> PrintOutputFormat {
    PrintOutputFormat::new()
}

pub struct PrintOutputFormat {
    task_id: TaskId,
    schema: Schema,
}

impl PrintOutputFormat {
    pub fn new() -> Self {
        PrintOutputFormat {
            task_id: TaskId::default(),
            schema: Schema::empty(),
        }
    }
}

impl OutputFormat for PrintOutputFormat {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        self.task_id = context.task_id;
        self.schema = context.input_schema.clone().into();

        Ok(())
    }

    fn write_record(&mut self, mut record: Record) {
        let reader = record.as_buffer().as_reader(self.schema.as_type_ids());
        let mut field_str_vec = Vec::new();
        for i in 0..self.schema.fields().len() {
            let field = self.schema.field(i);
            let field_str = match field.data_type() {
                DataType::Boolean => reader.get_bool(i).unwrap().to_string(),
                DataType::Int8 => reader.get_i8(i).unwrap().to_string(),
                DataType::UInt8 => reader.get_u8(i).unwrap().to_string(),
                DataType::Int16 => reader.get_i16(i).unwrap().to_string(),
                DataType::UInt16 => reader.get_i16(i).unwrap().to_string(),
                DataType::Int32 => reader.get_i32(i).unwrap().to_string(),
                DataType::UInt32 => reader.get_u32(i).unwrap().to_string(),
                DataType::Int64 => reader.get_i64(i).unwrap().to_string(),
                DataType::UInt64 => reader.get_u64(i).unwrap().to_string(),
                DataType::Float32 => reader.get_f32(i).unwrap().to_string(),
                DataType::Float64 => reader.get_f64(i).unwrap().to_string(),
                DataType::Binary => match reader.get_str(i) {
                    Ok(s) => s.to_owned(),
                    Err(_e) => format!("{:?}", reader.get_binary(i).unwrap()),
                },
                DataType::String => reader.get_str(i).unwrap().to_string(),
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

    fn close(&mut self) -> crate::core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::Empty
    }
}

impl NamedFunction for PrintOutputFormat {
    fn name(&self) -> &str {
        "PrintOutputFormat"
    }
}

impl CheckpointFunction for PrintOutputFormat {}
