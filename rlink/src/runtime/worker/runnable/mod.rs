use crate::api::element::Element;
use std::fmt::Debug;

pub mod filter_runnable;
pub mod key_by_runnable;
pub mod map_runnable;
pub mod reduce_runnable;
pub mod sink_runnable;
pub mod source_runnable;
pub mod watermark_assigner_runnable;
pub mod window_assigner_runnable;

use crate::runtime::worker::FunctionContext;
use crate::runtime::{JobDescriptor, TaskDescriptor};
use crate::utils::timer::WindowTimer;

use crate::api::checkpoint::FunctionSnapshotContext;
pub(crate) use filter_runnable::FilterRunnable;
pub(crate) use key_by_runnable::KeyByRunnable;
pub(crate) use map_runnable::MapRunnable;
pub(crate) use reduce_runnable::ReduceRunnable;
pub(crate) use sink_runnable::SinkRunnable;
pub(crate) use source_runnable::SourceRunnable;
pub(crate) use watermark_assigner_runnable::WatermarkAssignerRunnable;
pub(crate) use window_assigner_runnable::WindowAssignerRunnable;

// pub type RunnableContext = TaskDescriptor;

#[derive(Clone, Debug)]
pub(crate) struct RunnableContext {
    pub(crate) task_manager_id: String,
    pub(crate) job_descriptor: JobDescriptor,
    pub(crate) task_descriptor: TaskDescriptor,
    pub(crate) window_timer: WindowTimer,
}

impl RunnableContext {
    pub(crate) fn to_fun_context(&self) -> FunctionContext {
        FunctionContext {
            job_id: self.job_descriptor.job_manager.job_id.clone(),
            job_properties: self.job_descriptor.job_manager.job_properties.clone(),
            task_id: self.task_descriptor.task_id.clone(),
            task_number: self.task_descriptor.task_number,
            num_tasks: self.task_descriptor.num_tasks,
            chain_id: self.task_descriptor.chain_id,
            dependency_chain_id: self.task_descriptor.dependency_chain_id,
            checkpoint_id: self.task_descriptor.checkpoint_id,
            checkpoint_handle: self.task_descriptor.checkpoint_handle.clone(),
        }
    }

    pub(crate) fn get_checkpoint_context(&self, checkpoint_id: u64) -> FunctionSnapshotContext {
        FunctionSnapshotContext::new(
            self.task_descriptor.chain_id,
            self.task_descriptor.task_number,
            checkpoint_id,
        )
    }
}

pub(crate) trait Runnable: Debug {
    fn open(&mut self, context: &RunnableContext);
    fn run(&mut self, element: Element);
    fn close(&mut self);
    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>);
    fn checkpoint(&mut self, checkpoint_id: u64);
}
