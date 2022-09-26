use std::time::Duration;

use crate::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use crate::core::data_types::{DataType, Schema};
use crate::core::element::{Element, FnSchema};
use crate::core::function::{Context, NamedFunction, OutputFormat};
use crate::core::runtime::TaskId;
use crate::core::window::TWindow;
use crate::utils::date_time::{current_timestamp_millis, fmt_date_time};

pub fn print_sink() -> PrintOutputFormat {
    PrintOutputFormat::new()
}

pub struct PrintOutputFormat {
    task_id: TaskId,
    schema: Schema,
    header: String,
    laster_print_timestamp: u64,
}

impl PrintOutputFormat {
    pub fn new() -> Self {
        PrintOutputFormat {
            task_id: TaskId::default(),
            schema: Schema::empty(),
            header: "".to_string(),
            laster_print_timestamp: 0,
        }
    }
}

#[async_trait]
impl OutputFormat for PrintOutputFormat {
    async fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        self.task_id = context.task_id;
        self.schema = context.input_schema.clone().into();

        let field_names: Vec<String> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| format!("{}:{}", index, field.name()))
            .collect();
        self.header = field_names.join("|");

        Ok(())
    }

    async fn write_element(&mut self, element: Element) {
        let mut record = element.into_record();
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

            field_str_vec.push(format!("{}", field_str));
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

        let current_timestamp = current_timestamp_millis();
        if current_timestamp - self.laster_print_timestamp > 3000 {
            println!("task_number|window[start,end]|{}", self.header);
        }
        self.laster_print_timestamp = current_timestamp;

        println!(
            "{}, {}, {}",
            self.task_id.task_number,
            window_str,
            field_str_vec.join(", "),
        );
    }

    async fn close(&mut self) -> crate::core::Result<()> {
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

#[async_trait]
impl CheckpointFunction for PrintOutputFormat {
    async fn initialize_state(
        &mut self,
        _context: &FunctionSnapshotContext,
        _handle: &Option<CheckpointHandle>,
    ) {
    }

    async fn snapshot_state(
        &mut self,
        _context: &FunctionSnapshotContext,
    ) -> Option<CheckpointHandle> {
        None
    }
}
