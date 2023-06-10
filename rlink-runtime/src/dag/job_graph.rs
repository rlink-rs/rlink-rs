use std::collections::HashMap;
use std::ops::Index;

use daggy::{Dag, NodeIndex, Walker};
use rlink_core::job::{JobEdge, JobNode};
use rlink_core::JobId;

// #[derive(Clone, Serialize, Deserialize, Debug)]
// pub enum JobEdge {
//     /// Forward
//     Forward = 1,
//     /// Hash
//     ReBalance = 2,
// }
//
// #[derive(Clone, Serialize, Deserialize, Debug)]
// pub struct JobNode {
//     pub job_id: JobId,
//     pub parallelism: u16,
//     // pub child_job_ids: Vec<JobId>,
//     // pub parent_job_ids: Vec<JobId>,
//     pub daemon: bool,
// }

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JobGraph {
    #[serde(skip)]
    pub(crate) job_node_indies: HashMap<JobId, NodeIndex>,
    pub(crate) dag: Dag<JobNode, JobEdge>,
}

impl JobGraph {
    pub fn new(dag: Dag<JobNode, JobEdge>) -> Self {
        let mut job_node_indies = HashMap::new();
        for edge in dag.raw_edges() {
            let source = edge.source();
            job_node_indies.insert(dag.index(source).job_id, source);

            let target = edge.target();
            job_node_indies.insert(dag.index(target).job_id, target);
        }

        Self {
            job_node_indies,
            dag,
        }
    }

    pub fn children(&self, job_id: &JobId) -> Vec<(&JobNode, &JobEdge)> {
        let job_node_index = self.job_node_indies.get(job_id).unwrap();
        self.dag
            .children(*job_node_index)
            .iter(&self.dag)
            .map(|(edge, node)| (self.dag.index(node), self.dag.index(edge)))
            .collect()
    }
}
