use crate::api::operator::DEFAULT_PARALLELISM;
use crate::dag::stream_graph::{OperatorId, StreamNode};
use crate::dag::{DagError, Label, OperatorType, StreamGraph};
use daggy::{Dag, EdgeIndex, NodeIndex, Walker};
use std::cmp::max;
use std::collections::HashMap;
use std::ops::{Index, IndexMut};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum JobEdge {
    // TODO support
    // ReBalance = 0
    /// Forward
    Forward = 1,
    /// Hash
    Hash = 2,
}

impl Label for JobEdge {
    fn get_label(&self) -> String {
        format!("{:?}", self)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct JobNode {
    /// the first operator id in a pipeline as job_id
    pub(crate) job_id: u32,
    pub(crate) parallelism: u32,
    pub(crate) stream_nodes: Vec<StreamNode>,
    pub(crate) follower_job_ids: Vec<u32>,
    pub(crate) dependency_job_ids: Vec<u32>,
}

impl Label for JobNode {
    fn get_label(&self) -> String {
        format!("job:{}(p{})", self.job_id, self.parallelism)
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
}

#[derive(Debug)]
pub(crate) struct JobGraph {
    pub(crate) job_node_indies: HashMap<u32, NodeIndex>,
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
        self.build_job_nodes(stream_graph)?;
        debug!("{:?}", self.dag);
        self.build_job_edges()
    }

    pub fn build_job_edges(&mut self) -> Result<(), DagError> {
        let node_indies: Vec<NodeIndex> = self.job_node_indies.values().map(|x| *x).collect();
        node_indies.into_iter().for_each(|job_node_index| {
            self.build_job_edge(job_node_index);
        });

        Ok(())
    }

    fn build_job_edge(&mut self, job_node_index: NodeIndex) {
        let job_node = self.dag.index(job_node_index).clone();
        if job_node.follower_job_ids.len() == 0 {
            return;
        }

        for follower_job_id in &job_node.follower_job_ids {
            // update `dependency_job_ids`
            let follower_node_index = self.job_node_indies.get(follower_job_id).unwrap();
            let follower_job_node = self.dag.index(*follower_node_index);

            let job_edge = if job_node.is_reduce_job() {
                if job_node.parallelism != follower_job_node.parallelism {
                    unimplemented!("unsupported")
                }

                JobEdge::Forward
            } else {
                if job_node.parallelism == follower_job_node.parallelism {
                    JobEdge::Forward
                } else {
                    JobEdge::Hash
                }
            };

            self.dag
                .add_edge(job_node_index, *follower_node_index, job_edge)
                .unwrap();
        }
    }

    pub fn build_job_nodes(&mut self, stream_graph: &StreamGraph) -> Result<(), DagError> {
        let sources = stream_graph.sources.clone();
        let mut job_id_map = HashMap::new();
        for source_node_index in sources {
            let job_node = self.build_job_node(source_node_index, stream_graph)?;

            // build map: follower_job_id -> vec[dependency_job_id]
            for follower_job_id in &job_node.follower_job_ids {
                let dependency_job_ids = job_id_map.entry(*follower_job_id).or_insert(Vec::new());
                dependency_job_ids.push(job_node.job_id);
            }

            // build map: job_id -> NodeIndex
            let job_id = job_node.job_id;
            let node_index = self.dag.add_node(job_node);
            self.job_node_indies.insert(job_id, node_index);
        }

        for (follower_job_id, dependency_job_ids) in job_id_map {
            // update `dependency_job_ids`
            let node_index = self.job_node_indies.get(&follower_job_id).unwrap();
            let job_node = self.dag.index_mut(*node_index);
            job_node.dependency_job_ids = dependency_job_ids;

            // update parallelism
            let job_node = self.dag.index(*node_index).clone();
            if job_node.is_co_process_job() {
                let max_dep_parallelism = job_node
                    .dependency_job_ids
                    .iter()
                    .map(|dep_job_id| {
                        let dep_node_index = self.job_node_indies.get(dep_job_id).unwrap();
                        let dep_node_stream = self.dag.index(*dep_node_index);
                        dep_node_stream.parallelism
                    })
                    .max();
                match max_dep_parallelism {
                    Some(parallelism) => {
                        let job_node = self.dag.index_mut(*node_index);
                        job_node.parallelism = parallelism;
                    }
                    None => {
                        return Err(DagError::ParentOperatorNotFound);
                    }
                }
            }
            if job_node.is_reduce_job() {
                job_node
                    .follower_job_ids
                    .iter()
                    .for_each(|follower_job_id| {
                        let follower_node_index =
                            self.job_node_indies.get(follower_job_id).unwrap();
                        let follower_node_stream = self.dag.index_mut(*follower_node_index);
                        follower_node_stream.parallelism = job_node.parallelism;
                    })
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
        let mut job_id = 0;
        let mut follower_job_ids: Vec<OperatorId> = Vec::new();

        let mut node_index = source_node_index;
        let stream_dag = &stream_graph.dag;
        loop {
            let stream_node = stream_graph.get_stream_node(node_index);
            let children: Vec<(EdgeIndex, NodeIndex)> =
                stream_dag.children(node_index).iter(stream_dag).collect();

            stream_nodes.push(stream_node.clone());
            parallelism = max(parallelism, stream_node.parallelism);
            if stream_node.operator_type == OperatorType::Source {
                job_id = stream_node.id;
            }

            if stream_node.operator_type == OperatorType::Sink {
                let f_job_ids: Vec<OperatorId> = children
                    .iter()
                    .map(|(_edge_index, node_index)| stream_graph.get_stream_node(*node_index))
                    .map(|stream_node| stream_node.id)
                    .collect();
                follower_job_ids.extend_from_slice(f_job_ids.as_slice());
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
            follower_job_ids,
            dependency_job_ids: vec![],
        })
    }
}
