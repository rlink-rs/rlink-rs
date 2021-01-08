use crate::api::element::Record;
use crate::api::function::{Context, Function, OutputFormat};
use crate::dag::execution_graph::ExecutionEdge;
use crate::io::pub_sub::{publish, ChannelType};

pub struct SystemOutputFormat {}

impl OutputFormat for SystemOutputFormat {
    fn open(&mut self, context: &Context) {
        let mut memory_jobs = Vec::new();
        let mut network_jobs = Vec::new();

        context
            .children
            .iter()
            .for_each(|(execution_node, execution_edge)| match execution_edge {
                ExecutionEdge::Memory => memory_jobs.push(execution_node.task_id.clone()),
                ExecutionEdge::Network => network_jobs.push(execution_node.task_id.clone()),
            });

        if memory_jobs.len() > 0 {
            let n = publish(&context.task_id, &memory_jobs, ChannelType::Memory);
        }
        if network_jobs.len() > 0 {
            let n = publish(&context.task_id, &network_jobs, ChannelType::Network);
        }
    }

    fn write_record(&mut self, _record: Record) {}

    fn close(&mut self) {}
}

impl Function for SystemOutputFormat {
    fn get_name(&self) -> &str {
        "SystemOutputFormat"
    }
}
