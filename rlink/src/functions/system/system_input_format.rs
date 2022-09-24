use std::pin::Pin;
use std::sync::Arc;
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
use crate::runtime::worker::WorkerTaskContext;

pub(crate) struct SystemInputFormat {
    memory_receiver: Option<ElementReceiver>,
    network_receiver: Option<ElementReceiver>,

    task_id: TaskId,
    context: Option<Context>,
}

impl SystemInputFormat {
    pub fn new() -> Self {
        SystemInputFormat {
            memory_receiver: None,
            network_receiver: None,
            task_id: TaskId::default(),
            context: None,
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
        self.context = Some(context.clone());

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
            let rx = network::subscribe(
                context.task_context(),
                &network_jobs,
                &context.task_id,
                channel_size,
            );
            self.network_receiver = Some(rx);
        }

        Ok(())
    }

    async fn element_stream(&mut self) -> SendableElementStream {
        // if self.memory_receiver.is_none() && self.network_receiver.is_none() {
        //     panic!("unsupported")
        // } else if self.memory_receiver.is_some() && self.network_receiver.is_some() {
        //     let m = self.memory_receiver.take().unwrap();
        //     let n = self.network_receiver.take().unwrap();
        //     Box::pin(ChannelReceiverStream::new(m).merge(ChannelReceiverStream::new(n)))
        // } else if self.memory_receiver.is_some() {
        //     let m = self.memory_receiver.take().unwrap();
        //     Box::pin(ChannelReceiverStream::new(m))
        // } else {
        //     let n = self.network_receiver.take().unwrap();
        //     Box::pin(ChannelReceiverStream::new(n))
        // }

        let mut receivers = Vec::new();
        if let Some(n) = self.memory_receiver.take() {
            receivers.push(n);
        }
        if let Some(n) = self.network_receiver.take() {
            receivers.push(n);
        }

        Box::pin(InputChannelStream::new(
            receivers,
            self.context.as_ref().unwrap().task_context(),
        ))
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

struct InputChannelStream {
    n: usize,
    receiver: Vec<ElementReceiver>,
    task_context: Arc<WorkerTaskContext>,
}

impl InputChannelStream {
    pub fn new(receiver: Vec<ElementReceiver>, task_context: Arc<WorkerTaskContext>) -> Self {
        Self {
            n: 0,
            receiver,
            task_context,
        }
    }
}

impl ElementStream for InputChannelStream {}

impl Stream for InputChannelStream {
    type Item = Element;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.task_context.get_coordinator_status().is_terminated() {
            info!("MultiChannelIterator finish");
            return Poll::Ready(None);
        }

        let s = self.get_mut();

        let len = s.receiver.len();
        let first = {
            let first = s.n;
            s.n = first + 1;
            if s.n == len {
                s.n = 0;
            }
            first
        };

        for i in first..len {
            let receiver = s.receiver.get_mut(i).unwrap();
            match receiver.poll_recv(cx) {
                Poll::Ready(t) => {
                    return Poll::Ready(t);
                }
                _ => {}
            }
        }
        for i in 0..first {
            let receiver = s.receiver.get_mut(i).unwrap();
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
