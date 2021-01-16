use std::fmt::Debug;

use crate::api::checkpoint::FunctionSnapshotContext;
use crate::api::element::Element;
use crate::runtime::worker::FunctionContext;
use crate::runtime::{ApplicationDescriptor, TaskDescriptor};
use crate::utils::timer::WindowTimer;

pub mod co_process_runnable;
pub mod filter_runnable;
pub mod flat_map_runnable;
pub mod key_by_runnable;
pub mod reduce_runnable;
pub mod sink_runnable;
pub mod source_runnable;
pub mod watermark_assigner_runnable;
pub mod window_assigner_runnable;

use crate::api::runtime::{CheckpointId, OperatorId};
use crate::dag::job_graph::{JobEdge, JobNode};
use crate::dag::stream_graph::StreamNode;
use crate::dag::DagManager;
pub(crate) use filter_runnable::FilterRunnable;
pub(crate) use flat_map_runnable::FlatMapRunnable;
pub(crate) use key_by_runnable::KeyByRunnable;
pub(crate) use reduce_runnable::ReduceRunnable;
pub(crate) use sink_runnable::SinkRunnable;
pub(crate) use source_runnable::SourceRunnable;
pub(crate) use watermark_assigner_runnable::WatermarkAssignerRunnable;
pub(crate) use window_assigner_runnable::WindowAssignerRunnable;

#[derive(Clone, Debug)]
pub(crate) struct RunnableContext {
    pub(crate) dag_manager: DagManager,
    pub(crate) application_descriptor: ApplicationDescriptor,
    pub(crate) task_descriptor: TaskDescriptor,
    pub(crate) window_timer: WindowTimer,
}

impl RunnableContext {
    pub(crate) fn to_fun_context(&self, operator_id: OperatorId) -> FunctionContext {
        let coordinator_manager = &self.application_descriptor.coordinator_manager;
        FunctionContext {
            application_id: coordinator_manager.application_id.clone(),
            application_properties: coordinator_manager.application_properties.clone(),
            operator_id,
            task_id: self.task_descriptor.task_id.clone(),
            checkpoint_id: self.task_descriptor.checkpoint_id,
            checkpoint_handle: self.task_descriptor.checkpoint_handle.clone(),

            parents: self
                .dag_manager
                .get_task_parents(&self.task_descriptor.task_id),
            children: self
                .dag_manager
                .get_task_children(&self.task_descriptor.task_id),
        }
    }

    pub(crate) fn get_checkpoint_context(
        &self,
        operator_id: OperatorId,
        checkpoint_id: CheckpointId,
    ) -> FunctionSnapshotContext {
        FunctionSnapshotContext::new(operator_id, self.task_descriptor.task_id, checkpoint_id)
    }

    pub(crate) fn get_parent_parallelism(&self) -> u16 {
        let ps = self.get_parents_parallelism();
        *ps.get(0).unwrap()
    }

    pub(crate) fn get_parents_parallelism(&self) -> Vec<u16> {
        self.dag_manager
            .get_job_parents(self.task_descriptor.task_id.job_id)
            .iter()
            .map(|(job_node, _)| job_node.parallelism)
            .collect()
    }

    pub(crate) fn get_child_parallelism(&self) -> u16 {
        let ps = self.get_children_parallelism();
        *ps.get(0).unwrap()
    }

    pub(crate) fn get_children_parallelism(&self) -> Vec<u16> {
        self.dag_manager
            .get_job_children(self.task_descriptor.task_id.job_id)
            .iter()
            .map(|(job_node, _)| job_node.parallelism)
            .collect()
    }

    pub(crate) fn get_parent_jobs(&self) -> Vec<(JobNode, JobEdge)> {
        self.dag_manager
            .get_job_parents(self.task_descriptor.task_id.job_id)
            .iter()
            .map(|(job_node, job_edge)| (job_node.clone(), job_edge.clone()))
            .collect()
    }

    // pub(crate) fn get_parent_streams(&self, operator_id: u32) -> Vec<StreamNode> {
    //     self.dag_manager.get_stream_parents(operator_id)
    // }

    pub(crate) fn get_stream(&self, operator_id: OperatorId) -> StreamNode {
        self.dag_manager.get_stream(operator_id).unwrap()
    }
}

pub(crate) trait Runnable: Debug {
    fn open(&mut self, context: &RunnableContext);
    fn run(&mut self, element: Element);
    fn close(&mut self);
    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>);
    fn checkpoint(&mut self, checkpoint_id: CheckpointId);
}
