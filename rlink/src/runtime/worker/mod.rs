use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::ops::Deref;
use std::thread::JoinHandle;

use crate::api::element::{Element, Record};
use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::api::function::KeySelectorFunction;
use crate::api::operator::{StreamOperator, StreamOperatorWrap};
use crate::api::runtime::{JobId, OperatorId};
use crate::dag::{DagManager, OperatorType};
use crate::runtime::context::Context;
use crate::runtime::worker::runnable::co_process_runnable::CoProcessRunnable;
use crate::runtime::worker::runnable::{
    FilterRunnable, FlatMapRunnable, KeyByRunnable, ReduceRunnable, Runnable, RunnableContext,
    SinkRunnable, SourceRunnable, WatermarkAssignerRunnable, WindowAssignerRunnable,
};
use crate::runtime::{ApplicationDescriptor, TaskDescriptor};
use crate::storage::metadata::MetadataLoader;
use crate::utils::timer::WindowTimer;

pub mod checkpoint;
pub mod heart_beat;
pub mod runnable;

pub(crate) type FunctionContext = crate::api::function::Context;

pub(crate) fn run<S>(
    context: Context,
    metadata_loader: MetadataLoader,
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
            let stream_env = StreamExecutionEnvironment::new(application_name);
            let worker_task = WorkerTask::new(
                context,
                task_descriptor,
                metadata_loader,
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
    context: Context,
    task_descriptor: TaskDescriptor,
    application_descriptor: ApplicationDescriptor,
    metadata_loader: MetadataLoader,
    stream_app: S,
    stream_env: StreamExecutionEnvironment,
    window_timer: WindowTimer,
}

impl<S> WorkerTask<S>
where
    S: StreamApp + 'static,
{
    pub(crate) fn new(
        context: Context,
        task_descriptor: TaskDescriptor,
        mut metadata_loader: MetadataLoader,
        stream_app: S,
        stream_env: StreamExecutionEnvironment,
        window_timer: WindowTimer,
    ) -> Self {
        WorkerTask {
            context,
            task_descriptor,
            application_descriptor: metadata_loader.get_job_descriptor_from_cache(),
            metadata_loader,
            stream_app,
            stream_env,
            window_timer,
        }
    }

    pub fn run(mut self) -> anyhow::Result<()> {
        let application_properties = &self
            .application_descriptor
            .coordinator_manager
            .application_properties;
        self.stream_app
            .build_stream(application_properties, self.stream_env.borrow_mut());

        let mut raw_stream_graph = self.stream_env.stream_manager.stream_graph.borrow_mut();
        let dag_manager = DagManager::new(raw_stream_graph.deref());
        let operators = raw_stream_graph.pop_operators();

        let mut operator_invoke_chain = self.build_invoke_chain(&dag_manager, operators);
        debug!("Invoke: {:?}", operator_invoke_chain);

        let runnable_context = RunnableContext {
            dag_manager,
            application_descriptor: self.application_descriptor.clone(),
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
        dag_manager: &DagManager,
        mut operators: HashMap<OperatorId, StreamOperatorWrap>,
    ) -> Box<dyn Runnable> {
        let job_node = dag_manager
            .get_job_node(&self.task_descriptor.task_id)
            .expect(format!("Job={:?} is not found", &self.task_descriptor).as_str());

        let mut invoke_operators = Vec::new();
        for index in 0..job_node.stream_nodes.len() {
            let operator_id = job_node.stream_nodes[index].id;
            let operator = operators.remove(&operator_id).expect("operator not found");
            let invoke_operator = match operator {
                StreamOperatorWrap::StreamSource(stream_operator) => {
                    let op = SourceRunnable::new(
                        operator_id,
                        self.task_descriptor.input_split.clone(),
                        stream_operator,
                        None,
                    );
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamFlatMap(stream_operator) => {
                    let op = FlatMapRunnable::new(operator_id, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamFilter(stream_operator) => {
                    let op = FilterRunnable::new(operator_id, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamCoProcess(stream_operator) => {
                    let op = CoProcessRunnable::new(operator_id, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamKeyBy(stream_operator) => {
                    let op = KeyByRunnable::new(operator_id, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamReduce(stream_operator) => {
                    let stream_key_by = self.get_dependency_key_by(
                        &dag_manager,
                        operators.borrow_mut(),
                        job_node.job_id,
                    );
                    let op = ReduceRunnable::new(operator_id, stream_key_by, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamWatermarkAssigner(stream_operator) => {
                    let op = WatermarkAssignerRunnable::new(operator_id, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamWindowAssigner(stream_operator) => {
                    let op = WindowAssignerRunnable::new(operator_id, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamSink(stream_operator) => {
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
        dag_manager: &DagManager,
        operators: &mut HashMap<OperatorId, StreamOperatorWrap>,
        job_id: JobId,
    ) -> Option<StreamOperator<dyn KeySelectorFunction>> {
        let job_parents = dag_manager.get_job_parents(job_id);
        if job_parents.len() == 0 {
            error!("key by not found");
            None
        } else if job_parents.len() > 1 {
            error!("multi key by parents");
            None
        } else {
            let (job_node, _) = &job_parents[0];
            let stream_node = job_node
                .stream_nodes
                .iter()
                .find(|x| x.operator_type == OperatorType::KeyBy)
                .unwrap();
            let key_by_operator = operators.remove(&stream_node.id).unwrap();
            if let StreamOperatorWrap::StreamKeyBy(stream_operator) = key_by_operator {
                Some(stream_operator)
            } else {
                error!("dependency StreamKeyBy not found");
                None
            }
        }
    }
}
