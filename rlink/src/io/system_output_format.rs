use crate::api::element::Record;
use crate::api::function::{Context, Function, OutputFormat};

pub struct SystemOutputFormat {}

impl OutputFormat for SystemOutputFormat {
    fn open(&mut self, _context: &Context) {}

    fn write_record(&mut self, _record: Record) {}

    fn close(&mut self) {}
}

impl Function for SystemOutputFormat {
    fn get_name(&self) -> &str {
        "SystemOutputFormat"
    }
}
