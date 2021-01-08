use crate::api::element::Record;
use crate::api::function::{
    Context, Function, InputFormat, InputSplit, InputSplitAssigner, InputSplitSource,
};
use crate::api::properties::Properties;

pub struct SystemInputFormat {}

impl InputFormat for SystemInputFormat {
    fn open(&mut self, input_split: InputSplit, _context: &Context) {
        let _task_number = input_split.get_split_number();
    }

    fn reached_end(&self) -> bool {
        true
    }

    fn next_record(&mut self) -> Option<Record> {
        None
    }

    fn close(&mut self) {}
}

impl InputSplitSource for SystemInputFormat {
    fn create_input_splits(&self, min_num_splits: u32) -> Vec<InputSplit> {
        let mut input_splits = Vec::with_capacity(min_num_splits as usize);
        for task_number in 0..min_num_splits {
            input_splits.push(InputSplit::new(task_number, Properties::new()));
        }
        input_splits
    }

    fn get_input_split_assigner(&self, _input_splits: Vec<InputSplit>) -> InputSplitAssigner {
        unimplemented!()
    }
}

impl Function for SystemInputFormat {
    fn get_name(&self) -> &str {
        "SystemInputFormat"
    }
}
