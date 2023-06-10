use std::ops::Deref;

use crate::dag::dag_manager::DagManager;
use rlink_core::{JobId, TaskId};

use crate::dag::execution_graph::{ExecutionEdge, ExecutionNode};
use crate::dag::job_graph::JobNode;
use crate::dag::utils::{JsonDag, JsonNode};
use crate::JobEdge;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct DagMetadata {
    job_graph: JsonDag<JobNode, JobEdge>,
    execution_graph: JsonDag<ExecutionNode, ExecutionEdge>,
}

impl<'a> From<&'a DagManager> for DagMetadata {
    fn from(dag_manager: &'a DagManager) -> Self {
        DagMetadata {
            job_graph: JsonDag::from(&dag_manager.job_graph().dag),
            execution_graph: JsonDag::from(&dag_manager.execution_graph().dag),
        }
    }
}

impl ToString for DagMetadata {
    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

impl DagMetadata {
    pub fn job_graph(&self) -> &JsonDag<JobNode, JobEdge> {
        &self.job_graph
    }
    pub fn execution_graph(&self) -> &JsonDag<ExecutionNode, ExecutionEdge> {
        &self.execution_graph
    }
}

impl DagMetadata {
    pub fn job_node(&self, job_id: JobId) -> Option<&JobNode> {
        self.get_job_node(job_id).map(|node| node.deref())
    }

    fn get_job_node(&self, job_id: JobId) -> Option<&JsonNode<JobNode>> {
        self.job_graph
            .nodes()
            .iter()
            .find(|node| node.deref().job_id.eq(&job_id))
    }

    pub fn parent_jobs(&self, child_job_id: JobId) -> Vec<(&JobNode, &JobEdge)> {
        match self.get_job_node(child_job_id) {
            Some(node) => {
                let job_nodes: Vec<(&JobNode, &JobEdge)> = self
                    .job_graph
                    .parents(node.id())
                    .into_iter()
                    .map(|(node, edge)| (node.deref(), edge.deref()))
                    .collect();

                job_nodes
            }
            None => vec![],
        }
    }

    pub fn child_jobs(&self, parent_job_id: JobId) -> Vec<(&JobNode, &JobEdge)> {
        match self.get_job_node(parent_job_id) {
            Some(node) => {
                let job_nodes: Vec<(&JobNode, &JobEdge)> = self
                    .job_graph
                    .children(node.id())
                    .into_iter()
                    .map(|(node, edge)| (node.deref(), edge.deref()))
                    .collect();

                job_nodes
            }
            None => vec![],
        }
    }
}

impl DagMetadata {
    pub fn execution_parents(
        &self,
        child_task_id: &TaskId,
    ) -> Vec<(&ExecutionNode, &ExecutionEdge)> {
        match self.get_execution_node(child_task_id) {
            Some(node) => {
                let job_nodes: Vec<(&ExecutionNode, &ExecutionEdge)> = self
                    .execution_graph
                    .parents(node.id())
                    .into_iter()
                    .map(|(node, edge)| (node.deref(), edge.deref()))
                    .collect();

                job_nodes
            }
            None => vec![],
        }
    }

    pub fn execution_children(
        &self,
        parent_task_id: &TaskId,
    ) -> Vec<(&ExecutionNode, &ExecutionEdge)> {
        match self.get_execution_node(parent_task_id) {
            Some(node) => {
                let job_nodes: Vec<(&ExecutionNode, &ExecutionEdge)> = self
                    .execution_graph
                    .children(node.id())
                    .into_iter()
                    .map(|(node, edge)| (node.deref(), edge.deref()))
                    .collect();

                job_nodes
            }
            None => vec![],
        }
    }

    fn get_execution_node(&self, task_id: &TaskId) -> Option<&JsonNode<ExecutionNode>> {
        self.execution_graph
            .nodes()
            .iter()
            .find(|node| node.deref().task_id.eq(task_id))
    }
}
