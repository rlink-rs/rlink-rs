use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use crate::api::checkpoint::FunctionSnapshotContext;
use crate::api::element::Element;
use crate::api::properties::SystemProperties;
use crate::api::runtime::{CheckpointId, OperatorId};
use crate::dag::job_graph::{JobEdge, JobNode};
use crate::dag::metadata::DagMetadata;
use crate::dag::stream_graph::StreamNode;
use crate::runtime::timer::WindowTimer;
use crate::runtime::worker::FunctionContext;
use crate::runtime::{ApplicationDescriptor, TaskDescriptor};

pub mod co_process_runnable;
pub mod filter_runnable;
pub mod flat_map_runnable;
pub mod key_by_runnable;
pub mod reduce_runnable;
pub mod sink_runnable;
pub mod source_runnable;
pub mod watermark_assigner_runnable;
pub mod window_assigner_runnable;

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
    pub(crate) dag_metadata: Arc<DagMetadata>,
    pub(crate) application_descriptor: Arc<ApplicationDescriptor>,
    pub(crate) task_descriptor: TaskDescriptor,
    pub(crate) window_timer: WindowTimer,
}

impl RunnableContext {
    pub(crate) fn to_fun_context(&self, operator_id: OperatorId) -> FunctionContext {
        let coordinator_manager = &self.application_descriptor.coordinator_manager;
        let parents = self
            .dag_metadata
            .execution_parents(&self.task_descriptor.task_id)
            .into_iter()
            .map(|(node, edge)| (node.clone(), edge.clone()))
            .collect();
        let children = self
            .dag_metadata
            .execution_children(&self.task_descriptor.task_id)
            .into_iter()
            .map(|(node, edge)| (node.clone(), edge.clone()))
            .collect();

        FunctionContext {
            application_id: coordinator_manager.application_id.clone(),
            application_properties: coordinator_manager.application_properties.clone(),
            operator_id,
            task_id: self.task_descriptor.task_id.clone(),
            checkpoint_id: self.task_descriptor.checkpoint_id,
            checkpoint_handle: self.task_descriptor.checkpoint_handle.clone(),

            parents,
            children,
        }
    }

    pub(crate) fn checkpoint_context(
        &self,
        operator_id: OperatorId,
        checkpoint_id: CheckpointId,
    ) -> FunctionSnapshotContext {
        FunctionSnapshotContext::new(operator_id, self.task_descriptor.task_id, checkpoint_id)
    }

    pub(crate) fn checkpoint_internal(&self, default_value: Duration) -> Duration {
        self.application_descriptor
            .coordinator_manager
            .application_properties
            .get_checkpoint_internal()
            .unwrap_or(default_value)
    }

    pub(crate) fn parent_parallelism(&self) -> u16 {
        let ps = self.parents_parallelism();
        *ps.get(0).unwrap()
    }

    pub(crate) fn parents_parallelism(&self) -> Vec<u16> {
        self.dag_metadata
            .job_parents(self.task_descriptor.task_id.job_id)
            .iter()
            .map(|(job_node, _)| job_node.parallelism)
            .collect()
    }

    pub(crate) fn child_parallelism(&self) -> u16 {
        let ps = self.children_parallelism();
        *ps.get(0).unwrap()
    }

    pub(crate) fn children_parallelism(&self) -> Vec<u16> {
        self.dag_metadata
            .job_children(self.task_descriptor.task_id.job_id)
            .into_iter()
            .map(|(job_node, _)| job_node.parallelism)
            .collect()
    }

    pub(crate) fn parent_jobs(&self) -> Vec<(JobNode, JobEdge)> {
        self.dag_metadata
            .job_parents(self.task_descriptor.task_id.job_id)
            .into_iter()
            .map(|(job_node, job_edge)| (job_node.clone(), job_edge.clone()))
            .collect()
    }

    #[allow(dead_code)]
    pub(crate) fn stream_node(&self, operator_id: OperatorId) -> &StreamNode {
        self.dag_metadata.stream_node(operator_id).unwrap()
    }

    pub(crate) fn job_node(&self) -> &JobNode {
        self.dag_metadata
            .job_node(self.task_descriptor.task_id.job_id)
            .unwrap()
    }
}

pub(crate) trait Runnable: Debug {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()>;
    fn run(&mut self, element: Element);
    fn close(&mut self) -> anyhow::Result<()>;
    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>);
    fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext);
}
