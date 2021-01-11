use crate::api::element::{Element, Record};
use crate::api::function::{
    Context, Function, InputFormat, InputSplit, InputSplitAssigner, InputSplitSource,
};
use crate::api::properties::Properties;
use crate::api::runtime::TaskId;
use crate::channel::{ElementReceiver, TryRecvError};
use crate::dag::execution_graph::ExecutionEdge;
use crate::io::{memory, network};

pub struct SystemInputFormat {
    memory_receiver: Option<ElementReceiver>,
    network_receiver: Option<ElementReceiver>,

    task_id: TaskId,
}

impl SystemInputFormat {
    pub fn new() -> Self {
        SystemInputFormat {
            memory_receiver: None,
            network_receiver: None,
            task_id: TaskId::default(),
        }
    }
}

impl InputFormat for SystemInputFormat {
    fn open(&mut self, _input_split: InputSplit, context: &Context) {
        self.task_id = context.task_id.clone();
        let parents: Vec<String> = context
            .parents
            .iter()
            .map(|(node, edge)| {
                format!(
                    "Node: {:?} --{:?}--> {:?}",
                    node.task_id, edge, &context.task_id
                )
            })
            .collect();
        info!("subscribe\n   {}", parents.join("\n  "));

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
            let rx = memory::subscribe(&memory_jobs, &context.task_id);
            self.memory_receiver = Some(rx);
        }
        if network_jobs.len() > 0 {
            let rx = network::subscribe(&network_jobs, &context.task_id);
            self.network_receiver = Some(rx);
        }
    }

    fn reached_end(&self) -> bool {
        false
    }

    fn next_record(&mut self) -> Option<Record> {
        None
    }

    fn next_element(&mut self) -> Option<Element> {
        match &self.network_receiver {
            Some(network_receiver) => match network_receiver.try_recv() {
                Ok(element) => {
                    info!("receive element {:?}", &self.task_id);
                    return Some(element);
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    panic!("network_receiver Disconnected");
                }
            },
            None => {}
        }

        match &self.memory_receiver {
            Some(memory_receiver) => match memory_receiver.try_recv() {
                Ok(element) => {
                    info!("receive element {:?}", &self.task_id);
                    Some(element)
                }
                Err(TryRecvError::Empty) => None,
                Err(TryRecvError::Disconnected) => {
                    panic!("memory_receiver Disconnected");
                }
            },
            None => None,
        }
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
