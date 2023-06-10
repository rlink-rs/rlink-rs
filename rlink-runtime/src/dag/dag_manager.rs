use std::sync::Arc;

use daggy::Dag;
use rlink_core::job::{Job, JobEdge, JobFactory, JobNode};
use rlink_core::JobId;
use tokio::sync::Mutex;

use crate::dag;
use crate::dag::execution_graph::ExecutionGraph;
use crate::dag::job_graph::JobGraph;
use crate::dag::physic_graph::PhysicGraph;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DagMetadata {
    job_graph: JobGraph,
    execution_graph: ExecutionGraph,
    physic_graph: PhysicGraph,
}

impl DagMetadata {
    pub fn new(job_dag: Dag<JobNode, JobEdge>) -> Result<Self, dag::DagError> {
        let job_graph = JobGraph::new(job_dag);

        let mut execution_graph = ExecutionGraph::new();
        execution_graph.build(&job_graph)?;

        let mut physic_graph = PhysicGraph::new();
        physic_graph.build(&execution_graph);

        Ok(Self {
            job_graph,
            execution_graph,
            physic_graph,
        })
    }
}

#[derive(Clone, Debug)]
pub struct DagManager {
    job_factory: Arc<Mutex<Box<dyn JobFactory>>>,

    graph: Arc<DagMetadata>,
}

impl DagManager {
    pub fn new(
        job_dag: Dag<JobNode, JobEdge>,
        job_factory: Box<dyn JobFactory>,
    ) -> Result<Self, dag::DagError> {
        Ok(Self {
            job_factory: Arc::new(Mutex::new(job_factory)),
            graph: Arc::new(DagMetadata::new(job_dag)?),
        })
    }

    pub fn graph(&self) -> &DagMetadata {
        self.graph.as_ref()
    }

    pub fn job_graph(&self) -> &JobGraph {
        &self.graph.job_graph
    }

    pub fn execution_graph(&self) -> &ExecutionGraph {
        &self.graph.execution_graph
    }

    pub fn physic_graph(&self) -> &PhysicGraph {
        &self.graph.physic_graph
    }

    pub async fn create_job(&self, job_id: JobId) -> Box<dyn Job> {
        let mut job_factory = self.job_factory.lock().await;
        job_factory.create(job_id).await
    }
}

#[cfg(test)]
mod tests {
    use crate::dag::dag_manager::DagMetadata;
    use daggy::Dag;
    use rlink_core::job::{JobEdge, JobNode};
    use rlink_core::JobId;

    #[test]
    pub fn dag_manager_test() {
        let mut job_dag: Dag<JobNode, JobEdge> = Dag::new();
        let n0 = job_dag.add_node(JobNode::new(JobId(0), 2));
        let n1 = job_dag.add_node(JobNode::new(JobId(1), 1));
        let n2 = job_dag.add_node(JobNode::new(JobId(2), 2));
        let n3 = job_dag.add_node(JobNode::new(JobId(3), 3));
        let n4 = job_dag.add_node(JobNode::new(JobId(4), 3));

        job_dag.add_edge(n0, n2, JobEdge::Forward).unwrap();
        job_dag.add_edge(n1, n2, JobEdge::ReBalance).unwrap();
        job_dag.add_edge(n2, n3, JobEdge::ReBalance).unwrap();
        job_dag.add_edge(n2, n4, JobEdge::ReBalance).unwrap();

        let dag_metadata = DagMetadata::new(job_dag).unwrap();
        println!("{}", serde_json::to_string_pretty(&dag_metadata).unwrap());

        dag_metadata.physic_graph.alloc_by_instance(3);
    }
}
