use crate::api;
use crate::api::element::{Element, Record};
use crate::api::function::{Context, Function, InputFormat, InputSplit, InputSplitSource};
use crate::api::properties::SystemProperties;
use crate::api::runtime::TaskId;
use crate::channel::ElementReceiver;
use crate::dag::execution_graph::ExecutionEdge;
use crate::functions::iterator::{ChannelIterator, MultiChannelIterator};
use crate::pub_sub::{memory, network, DEFAULT_CHANNEL_SIZE};

pub(crate) struct SystemInputFormat {
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
    fn open(&mut self, _input_split: InputSplit, context: &Context) -> api::Result<()> {
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
        info!("subscribe\n  {}", parents.join("\n  "));

        let channel_size = context
            .application_properties
            .get_pub_sub_channel_size()
            .unwrap_or(DEFAULT_CHANNEL_SIZE);

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
            let rx = memory::subscribe(&memory_jobs, &context.task_id, channel_size);
            self.memory_receiver = Some(rx);
        }
        if network_jobs.len() > 0 {
            let rx = network::subscribe(&network_jobs, &context.task_id, channel_size);
            self.network_receiver = Some(rx);
        }

        Ok(())
    }

    fn record_iter(&mut self) -> Box<dyn Iterator<Item = Record> + Send> {
        unimplemented!()
    }

    fn element_iter(&mut self) -> Box<dyn Iterator<Item = Element> + Send> {
        let mut receivers = Vec::new();
        if let Some(n) = &self.memory_receiver {
            receivers.push(n.clone());
        }
        if let Some(n) = &self.network_receiver {
            receivers.push(n.clone());
        }

        match receivers.len() {
            0 => panic!("unsupported"),
            1 => Box::new(ChannelIterator::new(receivers.remove(0))),
            _ => Box::new(MultiChannelIterator::new(receivers)),
        }
    }

    fn close(&mut self) -> crate::api::Result<()> {
        Ok(())
    }
}

impl InputSplitSource for SystemInputFormat {}

impl Function for SystemInputFormat {
    fn get_name(&self) -> &str {
        "SystemInputFormat"
    }
}

struct SubscribeIterator {
    receiver: ElementReceiver,
}

impl Iterator for SubscribeIterator {
    type Item = Element;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(element) => {
                return Some(element);
            }
            Err(_e) => {
                panic!("network_receiver Disconnected");
            }
        }
    }
}
