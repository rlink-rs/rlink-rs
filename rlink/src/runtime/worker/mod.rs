use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::api::element::{Element, Record};
use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::api::function::KeySelectorFunction;
use crate::api::operator::{DefaultStreamOperator, StreamOperator};
use crate::api::runtime::{JobId, OperatorId};
use crate::dag::metadata::DagMetadata;
use crate::dag::OperatorType;
use crate::runtime::context::Context;
use crate::runtime::timer::WindowTimer;
use crate::runtime::worker::heart_beat::submit_heartbeat;
use crate::runtime::worker::runnable::co_process_runnable::CoProcessRunnable;
use crate::runtime::worker::runnable::{
    FilterRunnable, FlatMapRunnable, KeyByRunnable, ReduceRunnable, Runnable, RunnableContext,
    SinkRunnable, SourceRunnable, WatermarkAssignerRunnable, WindowAssignerRunnable,
};
use crate::runtime::{ClusterDescriptor, HeartbeatItem, TaskDescriptor};

pub mod checkpoint;
pub mod heart_beat;
pub mod runnable;

pub(crate) type FunctionContext = crate::api::function::Context;

pub(crate) fn run<S>(
    context: Arc<Context>,
    dag_metadata: Arc<DagMetadata>,
    cluster_descriptor: Arc<ClusterDescriptor>,
    task_descriptor: TaskDescriptor,
    stream_app: S,
    stream_env: &StreamExecutionEnvironment,
    window_timer: WindowTimer,
) -> JoinHandle<()>
where
    S: StreamApp + 'static,
{
    let application_name = stream_env.application_name.clone();
    std::thread::Builder::new()
        .name(format!(
            "RM-Task-{}-{}",
            task_descriptor.task_id.job_id.0, task_descriptor.task_id.task_number,
        ))
        .spawn(move || {
            submit_heartbeat(HeartbeatItem::TaskThreadId {
                task_id: task_descriptor.task_id.clone(),
                thread_id: thread_id::get() as u64,
            });

            let stream_env = StreamExecutionEnvironment::new(application_name);
            let worker_task = WorkerTask::new(
                context,
                dag_metadata,
                cluster_descriptor,
                task_descriptor,
                stream_app,
                stream_env,
                window_timer,
            );
            // todo error handle
            worker_task.run().unwrap();
        })
        .unwrap()
}

#[derive(Debug)]
pub struct WorkerTask<S>
where
    S: StreamApp + 'static,
{
    context: Arc<Context>,
    dag_metadata: Arc<DagMetadata>,
    cluster_descriptor: Arc<ClusterDescriptor>,
    task_descriptor: TaskDescriptor,
    stream_app: S,
    stream_env: StreamExecutionEnvironment,
    window_timer: WindowTimer,
}

impl<S> WorkerTask<S>
where
    S: StreamApp + 'static,
{
    pub(crate) fn new(
        context: Arc<Context>,
        dag_metadata: Arc<DagMetadata>,
        cluster_descriptor: Arc<ClusterDescriptor>,
        task_descriptor: TaskDescriptor,
        stream_app: S,
        stream_env: StreamExecutionEnvironment,
        window_timer: WindowTimer,
    ) -> Self {
        WorkerTask {
            context,
            dag_metadata,
            cluster_descriptor,
            task_descriptor,
            stream_app,
            stream_env,
            window_timer,
        }
    }

    pub fn run(mut self) -> anyhow::Result<()> {
        let application_properties = &self
            .cluster_descriptor
            .coordinator_manager
            .application_properties;
        self.stream_app
            .build_stream(application_properties, self.stream_env.borrow_mut());

        let mut raw_stream_graph = self.stream_env.stream_manager.stream_graph.borrow_mut();
        let operators = raw_stream_graph.pop_operators();

        let mut operator_invoke_chain = self.build_invoke_chain(operators);
        debug!("Invoke: {:?}", operator_invoke_chain);

        let runnable_context = RunnableContext {
            dag_metadata: self.dag_metadata.clone(),
            cluster_descriptor: self.cluster_descriptor.clone(),
            task_descriptor: self.task_descriptor.clone(),
            window_timer: self.window_timer.clone(),
        };

        info!("open Operator Chain");
        operator_invoke_chain.open(&runnable_context)?;

        info!("run Operator Chain");
        operator_invoke_chain.run(Element::Record(Record::new()));

        info!("close Operator Chain");
        operator_invoke_chain.close()?;

        Ok(())
    }

    fn build_invoke_chain(
        &self,
        mut operators: HashMap<OperatorId, StreamOperator>,
    ) -> Box<dyn Runnable> {
        let job_node = self
            .dag_metadata
            .job_node(self.task_descriptor.task_id.job_id)
            .expect(format!("Job={:?} is not found", &self.task_descriptor).as_str());

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
        let job_parents = self.dag_metadata.job_parents(job_id);
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
