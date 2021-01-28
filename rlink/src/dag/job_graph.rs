use std::cmp::max;
use std::collections::HashMap;
use std::ops::{Index, IndexMut};

use daggy::{Dag, EdgeIndex, NodeIndex, Walker};

use crate::api::operator::DEFAULT_PARALLELISM;
use crate::api::runtime::{JobId, OperatorId};
use crate::dag::stream_graph::{StreamGraph, StreamNode};
use crate::dag::utils::JsonDag;
use crate::dag::{utils, DagError, Label, OperatorType};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum JobEdge {
    /// Forward
    Forward = 1,
    /// Hash
    ReBalance = 2,
}

impl Label for JobEdge {
    fn get_label(&self) -> String {
        format!("{:?}", self)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct JobNode {
    /// the first operator id in a pipeline as job_id
    pub(crate) job_id: JobId,
    pub(crate) parallelism: u16,
    pub(crate) stream_nodes: Vec<StreamNode>,
    pub(crate) child_job_ids: Vec<JobId>,
    pub(crate) parent_job_ids: Vec<JobId>,
}

impl Label for JobNode {
    fn get_label(&self) -> String {
        let op_names: Vec<&str> = self
            .stream_nodes
            .iter()
            .map(|x| x.operator_name.as_str())
            .collect();
        let names: String = op_names.join(",\n");
        format!(
            "{:?}(parallelism:{})\n{}",
            self.job_id, self.parallelism, names
        )
    }
}

impl JobNode {
    fn is_co_process_job(&self) -> bool {
        self.stream_nodes
            .iter()
            .find(|stream_node| stream_node.operator_type == OperatorType::CoProcess)
            .is_some()
    }

    fn is_reduce_job(&self) -> bool {
        self.stream_nodes
            .iter()
            .find(|stream_node| stream_node.operator_type == OperatorType::Reduce)
            .is_some()
    }

    fn get_stream_node(&self, operator_id: OperatorId) -> Option<&StreamNode> {
        self.stream_nodes.iter().find(|x| x.id == operator_id)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct JobGraph {
    pub(crate) job_node_indies: HashMap<JobId, NodeIndex>,
    pub(crate) dag: Dag<JobNode, JobEdge>,
}

impl JobGraph {
    pub fn new() -> Self {
        JobGraph {
            job_node_indies: HashMap::new(),
            dag: Dag::new(),
        }
    }

    pub(crate) fn get_nodes(&self) -> Vec<JobNode> {
        utils::get_nodes(&self.dag)
    }

    pub(crate) fn get_job_node(&self, job_id: JobId) -> Option<JobNode> {
        self.job_node_indies.get(&job_id).map(|node_index| {
            let job_node = self.dag.index(*node_index);
            job_node.clone()
        })
    }

    pub(crate) fn get_parents(&self, job_id: JobId) -> Option<Vec<(JobNode, JobEdge)>> {
        self.job_node_indies.get(&job_id).map(|node_index| {
            let job_nodes: Vec<(JobNode, JobEdge)> = self
                .dag
                .parents(*node_index)
                .iter(&self.dag)
                .map(|(edge_index, node_index)| {
                    (
                        self.dag.index(node_index).clone(),
                        self.dag.index(edge_index).clone(),
                    )
                })
                .collect();
            job_nodes
        })
    }

    pub(crate) fn get_children(&self, job_id: JobId) -> Option<Vec<(JobNode, JobEdge)>> {
        self.job_node_indies.get(&job_id).map(|node_index| {
            let job_nodes: Vec<(JobNode, JobEdge)> = self
                .dag
                .children(*node_index)
                .iter(&self.dag)
                .map(|(edge_index, node_index)| {
                    (
                        self.dag.index(node_index).clone(),
                        self.dag.index(edge_index).clone(),
                    )
                })
                .collect();
            job_nodes
        })
    }

    pub fn build(&mut self, stream_graph: &StreamGraph) -> Result<(), DagError> {
        self.build_job_nodes(stream_graph)?;
        debug!("{:?}", self.dag);
        self.build_job_edges()
    }

    pub fn build_job_edges(&mut self) -> Result<(), DagError> {
        let node_indies: Vec<NodeIndex> = self.job_node_indies.values().map(|x| *x).collect();
        for job_node_index in node_indies {
            self.build_job_edge(job_node_index)?;
        }

        Ok(())
    }

    fn build_job_edge(&mut self, job_node_index: NodeIndex) -> Result<(), DagError> {
        let job_node = self.dag.index(job_node_index).clone();
        if job_node.child_job_ids.len() == 0 {
            return Ok(());
        }

        for child_job_id in &job_node.child_job_ids {
            let child_node_index = self
                .job_node_indies
                .get(child_job_id)
                .ok_or(DagError::JobNotFound(*child_job_id))?;
            let child_job_node = self.dag.index(*child_node_index);

            let job_edge = if child_job_node.is_reduce_job() {
                JobEdge::ReBalance
            } else if job_node.is_reduce_job() {
                if job_node.parallelism != child_job_node.parallelism {
                    return Err(DagError::ReduceOutputParallelismConflict);
                }

                JobEdge::Forward
            } else {
                if job_node.parallelism == child_job_node.parallelism {
                    JobEdge::Forward
                } else {
                    JobEdge::ReBalance
                }
            };

            self.dag
                .add_edge(job_node_index, *child_node_index, job_edge)
                .map_err(|_e| DagError::WouldCycle)?;
        }

        Ok(())
    }

    pub fn build_job_nodes(&mut self, stream_graph: &StreamGraph) -> Result<(), DagError> {
        let sources = stream_graph.sources.clone();

        // build jobs by StreamGraph
        let mut job_id_map = HashMap::new();
        for source_node_index in sources {
            let job_node = self.build_job_node(source_node_index, stream_graph)?;

            // build map: child_job_id -> vec[parent_job_id]
            for child_job_id in &job_node.child_job_ids {
                let parent_job_ids = job_id_map.entry(*child_job_id).or_insert(Vec::new());
                parent_job_ids.push(job_node.job_id);
            }

            // build map: job_id -> NodeIndex
            let job_id = job_node.job_id;
            let node_index = self.dag.add_node(job_node);
            self.job_node_indies.insert(job_id, node_index);
        }

        for (child_job_id, parent_job_ids) in job_id_map {
            let node_index = self
                .job_node_indies
                .get(&child_job_id)
                .ok_or(DagError::JobNotFound(child_job_id))?;

            // update `parent_job_ids`
            {
                let job_node = self.dag.index_mut(*node_index);
                job_node.parent_job_ids = parent_job_ids;
            }

            // update parallelism
            let job_node = self.dag.index(*node_index).clone();
            if job_node.is_co_process_job() {
                // the latest OperatorId is the left OperatorId
                let left_parent_id = {
                    let source_parent_ids = &job_node.stream_nodes[0].parent_ids;
                    *source_parent_ids
                        .get(source_parent_ids.len() - 1)
                        .ok_or(DagError::ConnectParentNotFound)?
                };

                // update the parallelism with parent left operator's parallelism
                let parent_parallelism = job_node
                    .parent_job_ids
                    .iter()
                    .map(|parent_job_id| {
                        let parent_node_index = self.job_node_indies.get(parent_job_id).unwrap();
                        self.dag.index(*parent_node_index)
                    })
                    .find(|job_node| job_node.get_stream_node(left_parent_id).is_some())
                    .map(|job_node| job_node.parallelism);
                match parent_parallelism {
                    Some(parallelism) => {
                        let job_node = self.dag.index_mut(*node_index);
                        job_node.parallelism = parallelism;
                    }
                    None => {
                        return Err(DagError::ParentOperatorNotFound);
                    }
                }
            } else if job_node.is_reduce_job() {
                for child_job_id in &job_node.child_job_ids {
                    let child_node_index = self
                        .job_node_indies
                        .get(child_job_id)
                        .ok_or(DagError::JobNotFound(*child_job_id))?;
                    let child_node_stream = self.dag.index_mut(*child_node_index);
                    child_node_stream.parallelism = job_node.parallelism;
                }
            }
        }

        // parallelism ==0 check, and inherit to parent's parallelism
        loop {
            let no_parallelism_jobs: Vec<(JobId, u16)> = self
                .job_node_indies
                .iter()
                .map(|(_job_id, node_index)| self.dag.index(*node_index))
                .filter(|job_node| job_node.parallelism == 0)
                .map(|job_node| {
                    if job_node.parent_job_ids.len() != 1 {
                        unimplemented!()
                    }
                    let parent_job_id = job_node.parent_job_ids.get(0).unwrap();

                    let parent_job_index = self.job_node_indies.get(parent_job_id).unwrap();
                    let parent_parallelism = self.dag.index(*parent_job_index).parallelism;
                    (job_node.job_id, parent_parallelism)
                })
                .collect();

            if no_parallelism_jobs.len() == 0 {
                break;
            }

            for (job_id, parent_parallelism) in no_parallelism_jobs {
                let node_index = self.job_node_indies.get(&job_id).unwrap();
                let job_node = self.dag.index_mut(*node_index);
                job_node.parallelism = parent_parallelism
            }
        }

        Ok(())
    }

    pub fn build_job_node(
        &mut self,
        source_node_index: NodeIndex,
        stream_graph: &StreamGraph,
    ) -> Result<JobNode, DagError> {
        let mut stream_nodes = Vec::new();
        let mut parallelism = DEFAULT_PARALLELISM;
        let mut job_id = JobId::default();
        let mut child_job_ids: Vec<JobId> = Vec::new();

        let mut node_index = source_node_index;
        let stream_dag = &stream_graph.dag;
        loop {
            let stream_node = stream_graph.get_stream_node(node_index);
            let children: Vec<(EdgeIndex, NodeIndex)> =
                stream_dag.children(node_index).iter(stream_dag).collect();

            stream_nodes.push(stream_node.clone());
            parallelism = max(parallelism, stream_node.parallelism);
            if stream_node.operator_type == OperatorType::Source {
                job_id = JobId::from(stream_node.id);
            }

            if stream_node.operator_type == OperatorType::Sink {
                let f_job_ids: Vec<JobId> = children
                    .iter()
                    .map(|(_edge_index, node_index)| stream_graph.get_stream_node(*node_index))
                    .map(|stream_node| JobId::from(stream_node.id))
                    .collect();
                child_job_ids.extend_from_slice(f_job_ids.as_slice());
                break;
            } else {
                if children.len() == 0 {
                    return Err(DagError::ChildNotFoundInPipeline);
                }

                if children.len() > 1 {
                    return Err(DagError::MultiChildrenInPipeline);
                }

                let child = children[0];
                node_index = child.1;
            }
        }

        Ok(JobNode {
            job_id,
            parallelism,
            stream_nodes,
            child_job_ids,
            parent_job_ids: vec![],
        })
    }

    pub(crate) fn to_string(&self) -> String {
        JsonDag::from(&self.dag).to_string()
    }
}
