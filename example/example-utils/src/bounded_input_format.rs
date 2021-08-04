use rlink::core;
use rlink::core::element::Record;
use rlink::core::function::{Context, InputFormat, InputSplit, InputSplitSource};

use crate::gen_record::gen_fix_length_records;

#[derive(Debug, Function)]
pub struct VecInputFormat {
    data: Vec<Record>,
}

impl VecInputFormat {
    pub fn new() -> Self {
        VecInputFormat { data: Vec::new() }
    }
}

impl InputSplitSource for VecInputFormat {}

impl InputFormat for VecInputFormat {
    fn open(&mut self, _input_split: InputSplit, context: &Context) -> core::Result<()> {
        let task_number = context.task_id.task_number() as usize;
        let num_tasks = context.task_id.num_tasks() as usize;

        let data = gen_fix_length_records();
        (0..data.len())
            .filter(|i| i % num_tasks == task_number)
            .for_each(|i| {
                self.data.push(data[i].clone());
            });

        Ok(())
    }

    fn record_iter(&mut self) -> Box<dyn Iterator<Item = Record> + Send> {
        Box::new(self.data.clone().into_iter())
    }

    fn close(&mut self) -> core::Result<()> {
        Ok(())
    }
}
