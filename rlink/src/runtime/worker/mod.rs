use crate::api::element::{Element, Record};
use crate::api::env::{StreamExecutionEnvironment, StreamJob};
use crate::api::function::KeySelectorFunction;
use crate::api::operator::{StreamOperator, StreamOperatorWrap};
use crate::graph::{build_logic_plan, JobGraph, OperatorChain};
use crate::runtime::context::Context;
use crate::runtime::worker::runnable::{
    FilterRunnable, KeyByRunnable, MapRunnable, ReduceRunnable, Runnable, RunnableContext,
    SinkRunnable, SourceRunnable, WatermarkAssignerRunnable, WindowAssignerRunnable,
};
use crate::runtime::{JobDescriptor, TaskDescriptor, TaskManagerStatus};
use crate::storage::metadata::MetadataLoader;
use crate::utils::timer::WindowTimer;
use std::borrow::BorrowMut;
use std::time::Duration;

pub mod checkpoint;
pub mod heart_beat;
pub mod io;
pub mod runnable;

pub(crate) type FunctionContext = crate::api::function::Context;

pub(crate) fn run<S>(
    context: Context,
    mut metadata_loader: MetadataLoader,
    task_descriptor: TaskDescriptor,
    stream_job: S,
    stream_env: StreamExecutionEnvironment,
    window_timer: WindowTimer,
) where
    S: StreamJob + 'static,
{
    std::thread::Builder::new()
        .name(format!(
            "TaskManager-Task-{}",
            task_descriptor.task_id.clone()
        ))
        .spawn(move || {
            waiting_all_task_manager_fine(metadata_loader.borrow_mut());

            WorkerTask::new(
                context,
                task_descriptor,
                metadata_loader,
                stream_job,
                stream_env,
                window_timer,
            )
            .run();
        })
        .unwrap();
}

fn waiting_all_task_manager_fine(metadata_loader: &mut MetadataLoader) {
    loop {
        let job_descriptor = metadata_loader.get_job_descriptor();
        match job_descriptor.job_manager.job_status {
            TaskManagerStatus::Registered => {
                break;
            }
            _ => std::thread::sleep(Duration::from_secs(2)),
        }
    }
}

#[derive(Debug)]
pub struct WorkerTask<S>
where
    S: StreamJob + 'static,
{
    context: Context,
    task_descriptor: TaskDescriptor,
    job_descriptor: JobDescriptor,
    metadata_loader: MetadataLoader,
    stream_job: S,
    stream_env: StreamExecutionEnvironment,
    window_timer: WindowTimer,
}

impl<S> WorkerTask<S>
where
    S: StreamJob + 'static,
{
    pub(crate) fn new(
        context: Context,
        task_descriptor: TaskDescriptor,
        mut metadata_loader: MetadataLoader,
        stream_job: S,
        stream_env: StreamExecutionEnvironment,
        window_timer: WindowTimer,
    ) -> Self {
        WorkerTask {
            context,
            task_descriptor,
            job_descriptor: metadata_loader.get_job_descriptor_from_cache(),
            metadata_loader,
            stream_job,
            stream_env,
            window_timer,
        }
    }

    pub fn run(self) {
        let data_stream = self.stream_job.build_stream(
            &self.job_descriptor.job_manager.job_properties,
            &self.stream_env,
        );
        debug!("DataStream: {:?}", data_stream);

        let logic_plan = build_logic_plan(data_stream, self.metadata_loader.clone());
        debug!("Logic Plan: {:?}", logic_plan);

        let mut operator_invoke_chain = self.build_invoke_chain(logic_plan);
        debug!("Invoke: {:?}", operator_invoke_chain);

        let runnable_context = RunnableContext {
            task_manager_id: self.context.task_manager_id.clone(),
            job_descriptor: self.job_descriptor.clone(),
            task_descriptor: self.task_descriptor.clone(),
            window_timer: self.window_timer.clone(),
        };

        info!("open Operator Chain");
        operator_invoke_chain.open(&runnable_context);

        info!("run Operator Chain");
        operator_invoke_chain.run(Element::Record(Record::new()));

        info!("close Operator Chain");
        operator_invoke_chain.close();
    }

    fn build_invoke_chain(&self, mut logic_plan: JobGraph) -> Box<dyn Runnable> {
        let chain_id = self.task_descriptor.chain_id;
        let operator_chain = logic_plan
            .chain_map
            .get(&chain_id)
            .expect(format!("Chain={} is not found", chain_id).as_str())
            .clone();

        let chain_nodes = operator_chain.nodes.clone();

        let mut invoke_operators = Vec::new();
        for index in 0..chain_nodes.len() {
            let operator_id = chain_nodes[index].node_id;
            let invoke_operator = match logic_plan.pop_stream_operator(operator_id) {
                StreamOperatorWrap::StreamSource(stream_operator) => {
                    let op = SourceRunnable::new(
                        self.task_descriptor.input_split.clone(),
                        stream_operator,
                        None,
                    );
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamMap(stream_operator) => {
                    let op = MapRunnable::new(stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamFilter(stream_operator) => {
                    let op = FilterRunnable::new(stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamKeyBy(stream_operator) => {
                    let partition_size = self.get_key_by_partition_size(&logic_plan, chain_id);
                    let op = KeyByRunnable::new(stream_operator, None, partition_size);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamReduce(stream_operator) => {
                    let stream_key_by = self
                        .get_dependency_key_by(&mut logic_plan, operator_chain.dependency_chain_id);
                    let op = ReduceRunnable::new(stream_key_by, stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamWatermarkAssigner(stream_operator) => {
                    let op = WatermarkAssignerRunnable::new(stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamWindowAssigner(stream_operator) => {
                    let op = WindowAssignerRunnable::new(stream_operator, None);
                    let op: Box<dyn Runnable> = Box::new(op);
                    op
                }
                StreamOperatorWrap::StreamSink(stream_operator) => {
                    let op = SinkRunnable::new(stream_operator);
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
        logic_plan: &mut JobGraph,
        dependency_chain_id: u32,
    ) -> Option<StreamOperator<dyn KeySelectorFunction>> {
        let dependency_operator_chain = logic_plan.chain_map.get(&dependency_chain_id);
        match dependency_operator_chain {
            Some(operator_chain) => operator_chain
                .nodes
                .clone()
                .iter()
                .find(|node| {
                    let operator_index = node.operator_index as usize;
                    let operator = &logic_plan.operators.get(operator_index).unwrap();
                    operator.is_key_by()
                })
                .map(|node| {
                    let key_by_operator = logic_plan.pop_stream_operator(node.node_id);
                    if let StreamOperatorWrap::StreamKeyBy(stream_operator) = key_by_operator {
                        stream_operator
                    } else {
                        panic!("dependency StreamKeyBy not found")
                    }
                }),
            None => None,
        }
    }

    fn get_key_by_partition_size(&self, logic_plan: &JobGraph, chain_id: u32) -> u16 {
        self.get_next_chain(logic_plan, chain_id)
            .expect("`KeyBy` Operator must be has the next `Chain`")
            .parallelism as u16
    }

    fn get_next_chain(&self, logic_plan: &JobGraph, chain_id: u32) -> Option<OperatorChain> {
        for (_chain_id, chain) in &logic_plan.chain_map {
            if chain.dependency_chain_id == chain_id {
                return Some(chain.clone());
            }
        }

        None
    }
}
