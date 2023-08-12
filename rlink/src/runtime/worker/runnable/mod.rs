use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use crate::core::checkpoint::FunctionSnapshotContext;
use crate::core::element::Element;
use crate::core::properties::SystemProperties;
use crate::core::runtime::{CheckpointId, JobId, OperatorId, TaskId};
use crate::dag::execution_graph::{ExecutionEdge, ExecutionNode};
use crate::dag::job_graph::{JobEdge, JobNode};
use crate::dag::metadata::DagMetadata;
use crate::dag::stream_graph::StreamNode;
use crate::runtime::worker::{FunctionContext, WorkerTaskContext};

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

#[derive(Clone)]
pub(crate) struct RunnableContext {
    pub(crate) task_context: Arc<WorkerTaskContext>,
}

impl RunnableContext {
    #[allow(unused)]
    pub fn task_context(&self) -> Arc<WorkerTaskContext> {
        self.task_context.clone()
    }

    fn job_id(&self) -> JobId {
        self.task_context.task_descriptor.task_id.job_id
    }

    fn dag_metadata(&self) -> &DagMetadata {
        self.task_context.dag_metadata.deref()
    }

    pub(crate) fn to_fun_context(&self, operator_id: OperatorId) -> FunctionContext {
        let dag_metadata = self.task_context.dag_metadata.deref();
        let coordinator_manager = &self.task_context.cluster_descriptor.coordinator_manager;
        let parents = dag_metadata
            .execution_parents(&self.task_context.task_descriptor.task_id)
            .into_iter()
            .map(|(node, edge)| (node.clone(), edge.clone()))
            .collect();
        let children = dag_metadata
            .execution_children(&self.task_context.task_descriptor.task_id)
            .into_iter()
            .map(|(node, edge)| (node.clone(), edge.clone()))
            .collect();
        let stream_node = dag_metadata.stream_node(operator_id).unwrap();

        let operator = self
            .task_context
            .task_descriptor
            .operators
            .iter()
            .find(|x| x.operator_id.eq(&operator_id))
            .unwrap();

        FunctionContext {
            application_id: coordinator_manager.application_id.clone(),
            application_properties: coordinator_manager.application_properties.clone(),
            operator_id,
            task_id: self.task_context.task_descriptor.task_id.clone(),
            checkpoint_id: operator.checkpoint_id,
            completed_checkpoint_id: operator.completed_checkpoint_id,
            checkpoint_handle: operator.checkpoint_handle.clone(),

            input_schema: stream_node.input_schema.clone(),
            output_schema: stream_node.output_schema.clone(),

            parents,
            children,

            task_context: Some(self.task_context.clone()),
        }
    }

    pub(crate) fn checkpoint_context(
        &self,
        operator_id: OperatorId,
        checkpoint_id: CheckpointId,
        completed_checkpoint_id: Option<CheckpointId>,
    ) -> FunctionSnapshotContext {
        FunctionSnapshotContext::new(
            operator_id,
            self.task_context.task_descriptor.task_id,
            checkpoint_id,
            completed_checkpoint_id,
            self.task_context.clone(),
        )
    }

    pub(crate) fn checkpoint_interval(&self, default_value: Duration) -> Duration {
        self.task_context
            .cluster_descriptor
            .coordinator_manager
            .application_properties
            .get_checkpoint_interval()
            .unwrap_or(default_value)
    }

    #[allow(dead_code)]
    pub(crate) fn parent_parallelism(&self) -> u16 {
        let ps = self.parents_parallelism();
        *ps.get(0).unwrap()
    }

    #[allow(dead_code)]
    pub(crate) fn parents_parallelism(&self) -> Vec<u16> {
        self.parent_jobs()
            .iter()
            .map(|(job_node, _)| job_node.parallelism)
            .collect()
    }

    pub(crate) fn child_parallelism(&self) -> u16 {
        let ps = self.children_parallelism();
        *ps.get(0).unwrap()
    }

    pub(crate) fn children_parallelism(&self) -> Vec<u16> {
        self.child_jobs()
            .into_iter()
            .map(|(job_node, _)| job_node.parallelism)
            .collect()
    }

    pub(crate) fn parent_jobs(&self) -> Vec<(&JobNode, &JobEdge)> {
        self.dag_metadata().parent_jobs(self.job_id())
    }

    pub(crate) fn child_jobs(&self) -> Vec<(&JobNode, &JobEdge)> {
        self.dag_metadata().child_jobs(self.job_id())
    }

    #[allow(dead_code)]
    pub(crate) fn stream_node(&self, operator_id: OperatorId) -> &StreamNode {
        self.dag_metadata().stream_node(operator_id).unwrap()
    }

    #[allow(dead_code)]
    pub(crate) fn job_node(&self) -> &JobNode {
        self.dag_metadata().job_node(self.job_id()).unwrap()
    }

    #[inline]
    pub(crate) fn parent_executions(
        &self,
        child_task_id: &TaskId,
    ) -> Vec<(&ExecutionNode, &ExecutionEdge)> {
        self.dag_metadata().execution_parents(child_task_id)
    }
}

#[async_trait]
pub(crate) trait Runnable: Send + Sync {
    async fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()>;
    async fn run(&mut self, element: Element);
    async fn close(&mut self) -> anyhow::Result<()>;
    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>);
    async fn checkpoint(&mut self, snapshot_context: FunctionSnapshotContext);
}
