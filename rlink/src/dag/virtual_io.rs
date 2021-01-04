use crate::api::element::Record;
use crate::api::function::{
    Context, Function, InputFormat, InputSplit, InputSplitAssigner, InputSplitSource, OutputFormat,
};

pub struct VirtualInputFormat {}

impl InputFormat for VirtualInputFormat {
    fn open(&mut self, _input_split: InputSplit, _context: &Context) {}

    fn reached_end(&self) -> bool {
        true
    }

    fn next_record(&mut self) -> Option<Record> {
        None
    }

    fn close(&mut self) {}
}

impl InputSplitSource for VirtualInputFormat {
    fn create_input_splits(&self, _min_num_splits: u32) -> Vec<InputSplit> {
        vec![]
    }

    fn get_input_split_assigner(&self, _input_splits: Vec<InputSplit>) -> InputSplitAssigner {
        unimplemented!()
    }
}

impl Function for VirtualInputFormat {
    fn get_name(&self) -> &str {
        "VirtualInputFormat"
    }
}

pub struct VirtualOutputFormat {}

impl OutputFormat for VirtualOutputFormat {
    fn open(&mut self, _context: &Context) {}

    fn write_record(&mut self, _record: Record) {}

    fn close(&mut self) {}
}

impl Function for VirtualOutputFormat {
    fn get_name(&self) -> &str {
        "VirtualOutputFormat"
    }
}
