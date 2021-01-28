use crate::api::runtime::{JobId, OperatorId, TaskId};
use crate::dag::execution_graph::{ExecutionEdge, ExecutionNode};
use crate::dag::job_graph::{JobEdge, JobNode};
use crate::dag::stream_graph::{StreamEdge, StreamNode};
use crate::dag::utils::{JsonDag, JsonNode};
use crate::dag::DagManager;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct DagMetadata {
    stream_graph: JsonDag<StreamNode, StreamEdge>,
    job_graph: JsonDag<JobNode, JobEdge>,
    execution_graph: JsonDag<ExecutionNode, ExecutionEdge>,
}

impl<'a> From<&'a DagManager> for DagMetadata {
    fn from(dag_manager: &'a DagManager) -> Self {
        DagMetadata {
            stream_graph: JsonDag::from(&dag_manager.stream_graph().dag),
            job_graph: JsonDag::from(&dag_manager.job_graph().dag),
            execution_graph: JsonDag::from(&dag_manager.execution_graph().dag),
        }
    }
}

impl DagMetadata {
    pub fn stream_graph(&self) -> &JsonDag<StreamNode, StreamEdge> {
        &self.stream_graph
    }
    pub fn job_graph(&self) -> &JsonDag<JobNode, JobEdge> {
        &self.job_graph
    }
    pub fn execution_graph(&self) -> &JsonDag<ExecutionNode, ExecutionEdge> {
        &self.execution_graph
    }
}

impl DagMetadata {
    pub fn get_stream(&self, operator_id: OperatorId) -> Option<&StreamNode> {
        self.get_stream0(operator_id).map(|node| node.detail())
    }

    fn get_stream0(&self, operator_id: OperatorId) -> Option<&JsonNode<StreamNode>> {
        self.stream_graph
            .nodes()
            .iter()
            .find(|node| node.detail().id.eq(&operator_id))
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////

    pub fn get_job_node(&self, job_id: JobId) -> Option<&JobNode> {
        self.get_job_node0(job_id).map(|node| node.detail())
    }

    fn get_job_node0(&self, job_id: JobId) -> Option<&JsonNode<JobNode>> {
        self.job_graph
            .nodes()
            .iter()
            .find(|node| node.detail().job_id.eq(&job_id))
    }

    pub fn get_job_parents(&self, job_id: JobId) -> Vec<(&JobNode, &JobEdge)> {
        self.get_jobs(job_id, true)
    }

    pub fn get_job_children(&self, job_id: JobId) -> Vec<(&JobNode, &JobEdge)> {
        self.get_jobs(job_id, false)
    }

    pub fn get_jobs(&self, job_id: JobId, parent: bool) -> Vec<(&JobNode, &JobEdge)> {
        match self.get_job_node0(job_id) {
            Some(node) => {
                let job_nodes: Vec<(&JobNode, &JobEdge)> = self
                    .job_graph
                    .edges()
                    .iter()
                    .filter(|edge| {
                        if parent {
                            edge.target().eq(node.id())
                        } else {
                            edge.source().eq(node.id())
                        }
                    })
                    .map(|edge| {
                        let node = self.job_graph.get_node(edge.target()).unwrap();
                        (node, edge)
                    })
                    .map(|(node, edge)| (node.detail(), edge.detail()))
                    .collect();

                job_nodes
            }
            None => vec![],
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////

    pub fn get_task_parents(&self, task_id: &TaskId) -> Vec<(&ExecutionNode, &ExecutionEdge)> {
        self.get_tasks(task_id, true)
    }

    pub fn get_task_children(&self, task_id: &TaskId) -> Vec<(&ExecutionNode, &ExecutionEdge)> {
        self.get_tasks(task_id, false)
    }

    fn get_tasks(&self, task_id: &TaskId, parent: bool) -> Vec<(&ExecutionNode, &ExecutionEdge)> {
        match self.get_task_node0(task_id) {
            Some(node) => {
                let job_nodes: Vec<(&ExecutionNode, &ExecutionEdge)> = self
                    .execution_graph
                    .edges()
                    .iter()
                    .filter(|edge| {
                        if parent {
                            edge.target().eq(node.id())
                        } else {
                            edge.source().eq(node.id())
                        }
                    })
                    .map(|edge| {
                        let node = self.execution_graph.get_node(edge.target()).unwrap();
                        (node, edge)
                    })
                    .map(|(node, edge)| (node.detail(), edge.detail()))
                    .collect();

                job_nodes
            }
            None => vec![],
        }
    }

    fn get_task_node0(&self, task_id: &TaskId) -> Option<&JsonNode<ExecutionNode>> {
        self.execution_graph
            .nodes()
            .iter()
            .find(|node| node.detail().task_id.eq(task_id))
    }
}
