use crate::api::operator::DEFAULT_PARALLELISM;
use crate::dag::stream_graph::OperatorId;
use crate::dag::{DagError, OperatorType, StreamGraph, StreamNode};
use daggy::{Dag, EdgeIndex, NodeIndex, Walker};
use std::cmp::max;
use std::collections::HashMap;
use std::ops::{Index, IndexMut};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum JobEdge {
    InSameTask = 1,
    CrossTask = 2,
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

#[derive(Debug)]
pub(crate) struct JobGraph {
    stream_graph: StreamGraph,

    job_node_indies: Vec<NodeIndex>,
    job_nodes: HashMap<u32, JobNode>,
    dag: Dag<JobNode, JobEdge>,
}

impl JobGraph {
    pub fn new(stream_graph: StreamGraph) -> Self {
        JobGraph {
            stream_graph,
            job_node_indies: Vec::new(),
            job_nodes: HashMap::new(),
            dag: Dag::new(),
        }
    }

    pub fn get_dag(&self) -> &Dag<JobNode, JobEdge> {
        &self.dag
    }

    pub fn build(&mut self) -> Result<(), DagError> {
        let job_node_indies = self.build_job_nodes()?;
        self.build_job_edges(job_node_indies)
    }

    pub fn build_job_edges(&mut self, job_node_indies: Vec<NodeIndex>) -> Result<(), DagError> {
        let mut job_nodes = HashMap::new();
        for job_node_index in &job_node_indies {
            let job_node = self.dag.index(*job_node_index);
            job_nodes.insert(job_node.job_id, *job_node_index);
        }

        for job_node_index in &job_node_indies {
            self.build_job_edge(*job_node_index, &job_nodes);
        }

        Ok(())
    }

    fn build_job_edge(&mut self, job_node_index: NodeIndex, job_nodes: &HashMap<u32, NodeIndex>) {
        let job_node = self.dag.index(job_node_index).clone();
        if job_node.follower_job_ids.len() == 0 {
            return;
        }

        for follower_job_id in job_node.follower_job_ids {
            // update `dependency_job_ids`
            let follower_node_index = job_nodes.get(&follower_job_id).unwrap();
            let follower_job_node = { self.dag.index_mut(*follower_node_index) };
            follower_job_node.dependency_job_ids.push(job_node.job_id);

            let follower_job_node = self.dag.index(*follower_node_index);
            let is_reduce_job = job_node
                .stream_nodes
                .iter()
                .find(|x| x.operator_type == OperatorType::Reduce)
                .is_some();

            let job_edge = if is_reduce_job {
                if job_node.parallelism == follower_job_node.parallelism {
                    unimplemented!("unsupported")
                }

                JobEdge::InSameTask
            } else {
                if job_node.parallelism == follower_job_node.parallelism {
                    JobEdge::InSameTask
                } else {
                    JobEdge::CrossTask
                }
            };

            self.dag
                .add_edge(job_node_index, *follower_node_index, job_edge)
                .unwrap();
        }
    }

    pub fn build_job_nodes(&mut self) -> Result<Vec<NodeIndex>, DagError> {
        let sources = self.stream_graph.sources.clone();
        let mut job_node_indies = Vec::new();
        for source_node_index in sources {
            let job_node = self.build_job_node(source_node_index)?;
            let node_index = self.dag.add_node(job_node);
            job_node_indies.push(node_index);
        }

        Ok(job_node_indies)
    }

    pub fn build_job_node(&mut self, source_node_index: NodeIndex) -> Result<JobNode, DagError> {
        let mut stream_nodes = Vec::new();
        let mut parallelism = DEFAULT_PARALLELISM;
        let mut job_id = 0;
        let mut follower_job_ids: Vec<OperatorId> = Vec::new();

        let mut node_index = source_node_index;
        let dag = self.stream_graph.get_dag();
        loop {
            let children: Vec<(EdgeIndex, NodeIndex)> =
                dag.children(node_index).iter(dag).collect();
            if children.len() == 0 {
                return Err(DagError::ChildNotFoundInPipeline);
            }
            if children.len() > 1 {
                return Err(DagError::MultiChildrenInPipeline);
            }

            let child = children[1];
            let node_index = child.1;

            let stream_node = self.stream_graph.get_stream_node(node_index);

            stream_nodes.push(stream_node.clone());
            parallelism = max(parallelism, stream_node.parallelism);
            if stream_node.operator_type == OperatorType::Source {
                job_id = stream_node.id;
            }

            if stream_node.operator_type == OperatorType::Sink {
                follower_job_ids = dag
                    .children(node_index)
                    .iter(dag)
                    .map(|(_edge_index, node_index)| self.stream_graph.get_stream_node(node_index))
                    .map(|stream_node| stream_node.id)
                    .collect();

                break;
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

    // pub fn build_source_jobs(&self, source_node_indies: Vec<NodeIndex>) -> Result<(), DagError> {
    //     if source_node_indies.len() == 0 {
    //         return Err(DagError::SourceNotFound);
    //     }
    //
    //     for source_node_index in source_node_indies {}
    //
    //     Ok(())
    // }

    // pub fn build_source_job(&self, source_node_index: NodeIndex) -> Result<(), DagError> {
    //     let stream_node = self.stream_graph.get_dag().index(source_node_index);
    //     let children: Vec<(EdgeIndex, NodeIndex)> = self
    //         .stream_graph
    //         .get_dag()
    //         .children(source_node_index)
    //         .iter(self.stream_graph.get_dag())
    //         .collect();
    //
    //     if children.len() == 0 {
    //         return Err(DagError::ChildNodeNotFound(stream_node.operator_type));
    //     }
    //
    //     if children.len() == 1 {}
    //
    //     Ok(())
    // }
    //
    // pub fn build_source_job0(
    //     &mut self,
    //     source_node_index: NodeIndex,
    // ) -> Result<NodeIndex, DagError> {
    //     let stream_node = self.stream_graph.get_dag().index(source_node_index);
    //     let parallelism = stream_node.parallelism;
    //     let children: Vec<(EdgeIndex, NodeIndex)> = self
    //         .stream_graph
    //         .get_dag()
    //         .children(source_node_index)
    //         .iter(self.stream_graph.get_dag())
    //         .collect();
    //
    //     if parallelism.value_type == ParValueType::Inherit {
    //         return Err(DagError::ParallelismInheritUnsupported(
    //             stream_node.operator_type,
    //         ));
    //     }
    //
    //     if children.len() == 0 {
    //         return Err(DagError::ChildNodeNotFound(stream_node.operator_type));
    //     }
    //
    //     let mut job_node = JobNode {
    //         job_id: self.job_id_gen,
    //         parallelism: parallelism.value,
    //         operator_ids: vec![stream_node.id],
    //     };
    //
    //     if children.len() > 0 {
    //         let source_index = self.dag.add_node(job_node);
    //         self.sources.push(source_index);
    //         return Ok(source_index);
    //     }
    //
    //     let child = *children.get(0).unwrap();
    //     let node_index = child.1;
    //     loop {
    //         let stream_node = self.stream_graph.get_dag().index(node_index);
    //         let children: Vec<(EdgeIndex, NodeIndex)> = self
    //             .stream_graph
    //             .get_dag()
    //             .children(node_index)
    //             .iter(self.stream_graph.get_dag())
    //             .collect();
    //
    //         job_node.operator_ids.push(stream_node.id);
    //
    //         if children.len() == 0 {
    //             return if stream_node.operator_type == OperatorType::Sink {
    //                 let source_index = self.dag.add_node(job_node);
    //                 self.sources.push(source_index);
    //                 Ok(source_index)
    //             } else {
    //                 Err(DagError::SinkNotFound)
    //             };
    //         } else if children.len() > 1 {
    //             let source_index = self.dag.add_node(job_node);
    //             self.sources.push(source_index);
    //             Ok(source_index)
    //         }
    //     }
    //
    //     Ok(())
    // }
}
