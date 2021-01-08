use crate::api::element::Record;
use crate::api::function::{
    Context, Function, InputFormat, InputSplit, InputSplitAssigner, InputSplitSource,
};
use crate::api::properties::Properties;
use crate::dag::execution_graph::ExecutionEdge;
use crate::io::pub_sub::{subscribe, ChannelType};

pub struct SystemInputFormat {}

impl InputFormat for SystemInputFormat {
    fn open(&mut self, input_split: InputSplit, context: &Context) {
        let _task_number = input_split.get_split_number();

        let mut memory_jobs = Vec::new();
        let mut network_jobs = Vec::new();
        context
            .parents
            .iter()
            .for_each(|(execution_node, execution_edge)| match execution_edge {
                ExecutionEdge::Memory => memory_jobs.push(execution_node.task_id.clone()),
                ExecutionEdge::Network => network_jobs.push(execution_node.task_id.clone()),
            });

        if memory_jobs.len() > 0 {
            let n = subscribe(&memory_jobs, &context.task_id, ChannelType::Memory);
        }
        if network_jobs.len() > 0 {
            let n = subscribe(&network_jobs, &context.task_id, ChannelType::Network);
        }
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
