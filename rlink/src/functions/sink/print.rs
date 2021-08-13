use std::time::Duration;

use serbuffer::types;

use crate::core::checkpoint::CheckpointFunction;
use crate::core::data_types::Schema;
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

        // let s: Vec<u8> = context.input_schema.clone().into();
        // if !s.as_slice().eq(self.field_types.as_slice()) {
        //     println!("{:?}", s,);
        //     println!("{:?}", self.field_types);
        //     panic!("00000");
        // }
        self.schema = context.input_schema.clone().into();

        Ok(())
    }

    fn write_record(&mut self, mut record: Record) {
        let reader = record.as_buffer().as_reader(self.schema.as_type_ids());
        let mut field_str_vec = Vec::new();
        for i in 0..self.schema.fields().len() {
            let field = self.schema.field(i);
            let field_str = match field.data_type_id() {
                types::BOOL => reader.get_bool(i).unwrap().to_string(),
                types::I8 => reader.get_i8(i).unwrap().to_string(),
                types::U8 => reader.get_u8(i).unwrap().to_string(),
                types::I16 => reader.get_i16(i).unwrap().to_string(),
                types::U16 => reader.get_i16(i).unwrap().to_string(),
                types::I32 => reader.get_i32(i).unwrap().to_string(),
                types::U32 => reader.get_u32(i).unwrap().to_string(),
                types::I64 => reader.get_i64(i).unwrap().to_string(),
                types::U64 => reader.get_u64(i).unwrap().to_string(),
                types::F32 => reader.get_f32(i).unwrap().to_string(),
                types::F64 => reader.get_f64(i).unwrap().to_string(),
                types::BINARY => match reader.get_str(i) {
                    Ok(s) => s.to_owned(),
                    Err(_e) => format!("{:?}", reader.get_binary(i).unwrap()),
                },
                types::STRING => reader.get_str(i).unwrap().to_string(),
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
