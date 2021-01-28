use crate::dag::execution_graph::{ExecutionEdge, ExecutionNode};
use crate::dag::job_graph::{JobEdge, JobNode};
use crate::dag::stream_graph::{StreamEdge, StreamNode};
use crate::dag::utils::JsonDag;
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
