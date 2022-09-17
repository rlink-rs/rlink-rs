use std::pin::Pin;
use std::task::Poll;

use futures::Stream;

use crate::channel::ElementReceiver;
use crate::core;
use crate::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::{Element, FnSchema};
use crate::core::function::{
    Context, ElementStream, InputFormat, InputSplit, InputSplitSource, NamedFunction,
    SendableElementStream,
};
use crate::core::properties::SystemProperties;
use crate::core::runtime::TaskId;
use crate::dag::execution_graph::ExecutionEdge;
use crate::pub_sub::{memory, network, DEFAULT_CHANNEL_SIZE};
use crate::runtime::worker::heart_beat::get_coordinator_status;

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

    fn subscribe_log(&self, context: &Context) {
        let mut parents: Vec<String> = context
            .parents
            .iter()
            .map(|(node, edge)| {
                format!(
                    "Node: {:?} --{:?}--> {:?}",
                    node.task_id, edge, &context.task_id
                )
            })
            .collect();
        parents.sort();
        info!("subscribe\n  {}", parents.join("\n  "));
    }
}

#[async_trait]
impl InputFormat for SystemInputFormat {
    async fn open(&mut self, _input_split: InputSplit, context: &Context) -> core::Result<()> {
        self.subscribe_log(context);

        self.task_id = context.task_id.clone();

        let channel_size = context
            .application_properties
            .get_pub_sub_channel_size()
            .unwrap_or(DEFAULT_CHANNEL_SIZE);
        // let channel_base_on = context
        //     .application_properties
        //     .get_pub_sub_channel_base()
        //     .unwrap_or(ChannelBaseOn::Bounded);

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
            let rx = network::subscribe(
                context.cluster_descriptor.clone(),
                &network_jobs,
                &context.task_id,
                channel_size,
            );
            self.network_receiver = Some(rx);
        }

        Ok(())
    }

    async fn element_stream(&mut self) -> SendableElementStream {
        let mut receivers = Vec::new();
        if let Some(n) = self.memory_receiver.take() {
            receivers.push(n);
        }
        if let Some(n) = self.network_receiver.take() {
            receivers.push(n);
        }

        // todo impl
        Box::pin(ElementReceiverStream::new(receivers))
        // match receivers.len() {
        //     0 => panic!("unsupported"),
        //     1 => Box::new(ChannelIterator::new(receivers.remove(0))),
        //     _ => Box::new(MultiChannelIterator::new(receivers)),
        // }
    }

    async fn close(&mut self) -> core::Result<()> {
        Ok(())
    }

    fn schema(&self, input_schema: FnSchema) -> FnSchema {
        input_schema
    }

    /// ignore
    fn parallelism(&self) -> u16 {
        0
    }
}

impl InputSplitSource for SystemInputFormat {}

impl NamedFunction for SystemInputFormat {
    fn name(&self) -> &str {
        "SystemInputFormat"
    }
}

#[async_trait]
impl CheckpointFunction for SystemInputFormat {
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

// struct ChannelIterator {
//     receiver: ElementReceiver,
// }
//
// impl ChannelIterator {
//     pub fn new(receiver: ElementReceiver) -> Self {
//         ChannelIterator { receiver }
//     }
// }
//
// impl Iterator for ChannelIterator {
//     type Item = Element;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         match self.receiver.recv() {
//             Ok(element) => {
//                 if get_coordinator_status().is_terminated() {
//                     info!("ChannelIterator finish");
//                     return None;
//                 }
//                 return Some(element);
//             }
//             Err(_e) => {
//                 panic!("network_receiver Disconnected");
//             }
//         }
//     }
// }
//
// pub struct MultiChannelIterator {
//     receivers: Vec<ElementReceiver>,
// }
//
// impl MultiChannelIterator {
//     pub fn new(receivers: Vec<ElementReceiver>) -> Self {
//         MultiChannelIterator { receivers }
//     }
// }
//
// impl Iterator for MultiChannelIterator {
//     type Item = Element;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         // Build a list of operations.
//         let mut sel = ChannelSelect::new();
//         for r in &self.receivers {
//             sel.recv(r);
//         }
//
//         loop {
//             // Wait until a receive operation becomes ready and try executing it.
//             let index = sel.ready();
//             let res = self.receivers[index].try_recv();
//
//             match res {
//                 Ok(element) => {
//                     if get_coordinator_status().is_terminated() {
//                         info!("MultiChannelIterator finish");
//                         return None;
//                     }
//                     return Some(element);
//                 }
//                 Err(TryRecvError::Empty) => continue,
//                 Err(TryRecvError::Disconnected) => panic!("the channel is Disconnected"),
//             }
//         }
//     }
// }

struct ElementReceiverStream {
    receiver: Vec<ElementReceiver>,
}

impl ElementReceiverStream {
    pub fn new(receiver: Vec<ElementReceiver>) -> Self {
        Self { receiver }
    }
}

impl ElementStream for ElementReceiverStream {}

impl Stream for ElementReceiverStream {
    type Item = Element;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if get_coordinator_status().is_terminated() {
            info!("MultiChannelIterator finish");
            return Poll::Ready(None);
        }

        for receiver in self.get_mut().receiver.as_mut_slice() {
            match receiver.poll_recv(cx) {
                Poll::Ready(t) => {
                    return Poll::Ready(t);
                }
                _ => {}
            }
        }

        Poll::Pending
    }
}
