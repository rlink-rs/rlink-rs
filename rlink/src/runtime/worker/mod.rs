use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::core::element::{Element, Record};
use crate::core::env::{StreamApp, StreamExecutionEnvironment};
use crate::core::function::KeySelectorFunction;
use crate::core::operator::{DefaultStreamOperator, StreamOperator};
use crate::core::runtime::{ClusterDescriptor, JobId, ManagerStatus, OperatorId, TaskDescriptor};
use crate::dag::metadata::DagMetadata;
use crate::dag::OperatorType;
use crate::runtime::context::Context;
use crate::runtime::timer::WindowTimer;
use crate::runtime::worker::checkpoint::CheckpointPublish;
use crate::runtime::worker::heart_beat::HeartbeatPublish;
use crate::runtime::worker::runnable::co_process_runnable::CoProcessRunnable;
use crate::runtime::worker::runnable::{
    FilterRunnable, FlatMapRunnable, KeyByRunnable, ReduceRunnable, Runnable, RunnableContext,
    SinkRunnable, SourceRunnable, WatermarkAssignerRunnable, WindowAssignerRunnable,
};
use crate::runtime::HeartbeatItem;

pub mod checkpoint;
pub mod heart_beat;
pub mod runnable;
pub mod web_server;

#[derive(Clone, Debug)]
pub(crate) struct WorkerTaskContext {
    #[allow(unused)]
    context: Arc<Context>,
    dag_metadata: Arc<DagMetadata>,
    cluster_descriptor: Arc<ClusterDescriptor>,
    task_descriptor: TaskDescriptor,

    window_timer: WindowTimer,
    checkpoint_publish: Arc<CheckpointPublish>,
    #[allow(unused)]
    heartbeat_publish: Arc<HeartbeatPublish>,
}

impl WorkerTaskContext {
    pub fn new(
        context: Arc<Context>,
        dag_metadata: Arc<DagMetadata>,
        cluster_descriptor: Arc<ClusterDescriptor>,
        task_descriptor: TaskDescriptor,
        window_timer: WindowTimer,
        checkpoint_publish: Arc<CheckpointPublish>,
        heartbeat_publish: Arc<HeartbeatPublish>,
    ) -> Self {
        Self {
            context,
            dag_metadata,
            cluster_descriptor,
            task_descriptor,
            window_timer,
            checkpoint_publish,
            heartbeat_publish,
        }
    }

    pub fn get_coordinator_status(&self) -> ManagerStatus {
        self.heartbeat_publish.get_coordinator_status()
    }

    #[allow(unused)]
    pub fn context(&self) -> Arc<Context> {
        self.context.clone()
    }

    #[allow(unused)]
    pub fn dag_metadata(&self) -> Arc<DagMetadata> {
        self.dag_metadata.clone()
    }
    pub fn cluster_descriptor(&self) -> Arc<ClusterDescriptor> {
        self.cluster_descriptor.clone()
    }

    #[allow(unused)]
    pub fn task_descriptor(&self) -> &TaskDescriptor {
        &self.task_descriptor
    }

    #[allow(unused)]
    pub fn window_timer(&self) -> WindowTimer {
        self.window_timer.clone()
    }
    pub fn checkpoint_publish(&self) -> Arc<CheckpointPublish> {
        self.checkpoint_publish.clone()
    }

    #[allow(unused)]
    pub fn heartbeat_publish(&self) -> Arc<HeartbeatPublish> {
        self.heartbeat_publish.clone()
    }
}

pub(crate) type FunctionContext = crate::core::function::Context;

pub(crate) async fn run<S>(task_context: Arc<WorkerTaskContext>, stream_app: S) -> JoinHandle<()>
where
    S: StreamApp + 'static,
{
    tokio::spawn(async move {
        task_context
            .heartbeat_publish()
            .report(HeartbeatItem::TaskThreadId {
                task_id: task_context.task_descriptor.task_id.clone(),
                thread_id: thread_id::get() as u64,
            });

        // let stream_env = StreamExecutionEnvironment::new();
        let worker_task = WorkerTask::new(task_context, stream_app);
        // todo error handle
        worker_task.run().await.unwrap();
    })
}

pub struct WorkerTask<S>
where
    S: StreamApp + 'static,
{
    task_context: Arc<WorkerTaskContext>,
    stream_app: S,
}

impl<S> WorkerTask<S>
where
    S: StreamApp + 'static,
{
    pub(crate) fn new(task_context: Arc<WorkerTaskContext>, stream_app: S) -> Self {
        WorkerTask {
            task_context,
            stream_app,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let application_properties = &self
            .task_context
            .cluster_descriptor
            .coordinator_manager
            .application_properties;
        let operators = {
            let mut stream_env = StreamExecutionEnvironment::new();
            self.stream_app
                .build_stream(application_properties, stream_env.borrow_mut());

            let mut raw_stream_graph = stream_env.stream_manager.stream_graph.borrow_mut();
            raw_stream_graph.pop_operators()
        };

        let mut operator_invoke_chain = self.build_invoke_chain(operators);
        // debug!("Invoke: {:?}", operator_invoke_chain);

        let runnable_context = RunnableContext {
            task_context: self.task_context.clone(),
        };

        info!("open Operator Chain");
        operator_invoke_chain.open(&runnable_context).await?;

        info!("run Operator Chain");
        operator_invoke_chain
            .run(Element::Record(Record::new()))
            .await;

        info!("close Operator Chain");
        operator_invoke_chain.close().await?;

        Ok(())
    }

    fn build_invoke_chain(
        &self,
        mut operators: HashMap<OperatorId, StreamOperator>,
    ) -> Box<dyn Runnable> {
        let job_node = self
            .task_context
            .dag_metadata
            .job_node(self.task_context.task_descriptor.task_id.job_id)
            .expect(format!("Job={:?} is not found", &self.task_context.task_descriptor).as_str());

        let mut invoke_operators = Vec::new();
        for index in 0..job_node.stream_nodes.len() {
            let operator_id = job_node.stream_nodes[index].id;
            let operator = operators.remove(&operator_id).expect("operator not found");
            let invoke_operator = match operator {
                StreamOperator::StreamSource(stream_operator) => {
                    let op = SourceRunnable::new(operator_id, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperator::StreamFlatMap(stream_operator) => {
                    let op = FlatMapRunnable::new(operator_id, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperator::StreamFilter(stream_operator) => {
                    let op = FilterRunnable::new(operator_id, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperator::StreamCoProcess(stream_operator) => {
                    let op = CoProcessRunnable::new(operator_id, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperator::StreamKeyBy(stream_operator) => {
                    let op = KeyByRunnable::new(operator_id, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperator::StreamReduce(stream_operator) => {
                    let stream_key_by =
                        self.get_dependency_key_by(operators.borrow_mut(), job_node.job_id);
                    let op = ReduceRunnable::new(operator_id, stream_key_by, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperator::StreamWatermarkAssigner(stream_operator) => {
                    let op = WatermarkAssignerRunnable::new(operator_id, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperator::StreamWindowAssigner(stream_operator) => {
                    let op = WindowAssignerRunnable::new(operator_id, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperator::StreamSink(stream_operator) => {
                    let op = SinkRunnable::new(operator_id, stream_operator);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
            };
            // invoke_operator.open();
            invoke_operators.push(invoke_operator);
        }

        loop {
            let op_dependency = invoke_operators.pop().unwrap();
            match invoke_operators.pop() {
                Some(mut invoke_operator) => {
                    invoke_operator.set_next_runnable(Some(op_dependency));
                    // let new_op = invoke_operator.into_new_operator(op_dependency);
                    invoke_operators.push(invoke_operator);
                }
                None => {
                    return op_dependency;
                }
            }
        }
    }

    fn get_dependency_key_by(
        &self,
        operators: &mut HashMap<OperatorId, StreamOperator>,
        job_id: JobId,
    ) -> Option<DefaultStreamOperator<dyn KeySelectorFunction>> {
        let job_parents = self.task_context.dag_metadata.job_parents(job_id);
        if job_parents.len() == 0 {
            error!("key by not found");
            None
        } else if job_parents.len() > 1 {
            error!("multi key by parents");
            None
        } else {
            let (job_node, _) = job_parents[0];
            let stream_node = job_node
                .stream_nodes
                .iter()
                .find(|x| x.operator_type == OperatorType::KeyBy)
                .unwrap();
            let key_by_operator = operators.remove(&stream_node.id).unwrap();
            if let StreamOperator::StreamKeyBy(stream_operator) = key_by_operator {
                Some(stream_operator)
            } else {
                error!("dependency StreamKeyBy not found");
                None
            }
        }
    }
}
