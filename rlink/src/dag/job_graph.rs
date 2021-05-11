use std::cmp::max;
use std::collections::{BTreeMap, HashMap};
use std::ops::Index;

use daggy::{Dag, EdgeIndex, NodeIndex, Walker};

use crate::core::operator::DEFAULT_PARALLELISM;
use crate::core::runtime::{JobId, OperatorId};
use crate::dag::stream_graph::{StreamGraph, StreamNode};
use crate::dag::{DagError, OperatorType};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum JobEdge {
    /// Forward
    Forward = 1,
    /// Hash
    ReBalance = 2,
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

    #[allow(dead_code)]
    fn stream_node(&self, operator_id: OperatorId) -> Option<&StreamNode> {
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

    pub fn build(&mut self, stream_graph: &StreamGraph) -> Result<(), DagError> {
        let mut builder = JobNodeBuilder::new(stream_graph);
        builder.build()?;
        for (job_id, job_node) in builder.job_node_indies {
            let node_index = self.dag.add_node(job_node);
            self.job_node_indies.insert(job_id, node_index);
        }

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
}

struct JobNodeBuilder<'a> {
    stream_graph: &'a StreamGraph,
    /// child to parents map
    c2p_job_id_map: BTreeMap<JobId, Vec<JobId>>,
    job_node_indies: HashMap<JobId, JobNode>,
}

impl<'a> JobNodeBuilder<'a> {
    pub fn new(stream_graph: &'a StreamGraph) -> Self {
        JobNodeBuilder {
            stream_graph,
            c2p_job_id_map: BTreeMap::new(),
            job_node_indies: HashMap::new(),
        }
    }

    fn build(&mut self) -> Result<(), DagError> {
        self.build_job_nodes()?;
        self.build_parallelism();
        Ok(())
    }

    fn build_job_nodes(&mut self) -> Result<(), DagError> {
        for source_node_index in &self.stream_graph.sources {
            let job_node = self.build_job_node(*source_node_index)?;

            // build map: child_job_id -> vec[parent_job_id]
            for child_job_id in &job_node.child_job_ids {
                let parent_job_ids = self
                    .c2p_job_id_map
                    .entry(*child_job_id)
                    .or_insert(Vec::new());
                parent_job_ids.push(job_node.job_id);
            }

            // build map: job_id -> NodeIndex
            self.job_node_indies.insert(job_node.job_id, job_node);
        }

        let c2p_job_id_map = self.c2p_job_id_map.clone();
        self.job_node_indies.iter_mut().for_each(|(_, job_node)| {
            let parent_job_ids = c2p_job_id_map
                .get(&job_node.job_id)
                .map(|x| x.clone())
                .unwrap_or_default();
            job_node.parent_job_ids = parent_job_ids;
        });

        Ok(())
    }

    pub fn build_job_node(&mut self, source_node_index: NodeIndex) -> Result<JobNode, DagError> {
        let mut stream_nodes = Vec::new();
        let mut parallelism = DEFAULT_PARALLELISM;
        let mut job_id = JobId::default();
        let mut child_job_ids: Vec<JobId> = Vec::new();

        let mut node_index = source_node_index;
        let stream_dag = &self.stream_graph.dag;
        loop {
            let stream_node = self.stream_graph.stream_node(node_index);
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
                    .map(|(_edge_index, node_index)| self.stream_graph.stream_node(*node_index))
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

    fn build_parallelism(&mut self) {
        while self.get_parent_parallelism() > 0 {}
    }

    fn get_parent_parallelism(&mut self) -> usize {
        let mut no_parallelism_jobs: Vec<JobId> = self
            .job_node_indies
            .iter()
            .filter_map(|(_, node)| {
                if node.parallelism == 0 {
                    Some(node.job_id)
                } else {
                    None
                }
            })
            .collect();
        no_parallelism_jobs.sort_by(|x, y| y.cmp(x));

        for job_id in &no_parallelism_jobs {
            let job_node = self.job_node_indies.get(job_id).unwrap();
            if job_node.parallelism > 0 {
                continue;
            }

            let job_node = job_node.clone();
            self.get_parent_parallelism0(job_node.clone());
        }

        no_parallelism_jobs.len()
    }

    fn get_parent_parallelism0(&mut self, job_node: JobNode) -> u16 {
        match self.c2p_job_id_map.get(&job_node.job_id) {
            Some(parent_job_ids) => {
                let parent_job_ids = parent_job_ids.clone();
                let parallelism = if parent_job_ids.len() == 1 {
                    let parent_job_node = self
                        .job_node_indies
                        .get(&parent_job_ids[0])
                        .unwrap()
                        .clone();
                    let parallelism = if parent_job_node.parallelism > 0 {
                        parent_job_node.parallelism
                    } else {
                        self.get_parent_parallelism0(parent_job_node)
                    };

                    parallelism
                } else if job_node.is_co_process_job() {
                    // connect node's parallelism = left parent node's parallelism
                    let mut parallelism = 0;
                    let left_job_id = self.get_parent_job_id(&job_node);

                    for parent_job_id in parent_job_ids {
                        let parent_job_node =
                            self.job_node_indies.get(&parent_job_id).unwrap().clone();
                        let p = if parent_job_node.parallelism > 0 {
                            parent_job_node.parallelism
                        } else {
                            self.get_parent_parallelism0(parent_job_node)
                        };
                        if parent_job_id == left_job_id {
                            parallelism = p
                        }
                    }

                    parallelism
                } else {
                    unimplemented!()
                };

                {
                    let jn = self.job_node_indies.get_mut(&job_node.job_id).unwrap();
                    jn.parallelism = parallelism;
                }
            }
            None => {}
        }

        0
    }

    fn get_parent_job_id(&self, job_node: &JobNode) -> JobId {
        let source_stream_nodes = &job_node.stream_nodes[0];
        let parent_operator_id =
            source_stream_nodes.parent_ids[source_stream_nodes.parent_ids.len() - 1];

        self.get_job_id_by_sink(parent_operator_id).unwrap()
    }

    fn get_job_id_by_sink(&self, sink_operator_id: OperatorId) -> Option<JobId> {
        for (_, job_node) in &self.job_node_indies {
            let sink_stream_node = &job_node.stream_nodes[job_node.stream_nodes.len() - 1];
            if sink_stream_node.id == sink_operator_id {
                return Some(job_node.job_id);
            }
        }
        None
    }
}
