use std::collections::HashMap;
use std::ops::Index;

use daggy::{Dag, NodeIndex};
use rlink_core::job::{ExecutionEdge, ExecutionNode, JobEdge};
use rlink_core::{JobId, TaskId};

use crate::dag::job_graph::JobGraph;
use crate::dag::DagError;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionGraph {
    #[serde(skip)]
    pub(crate) node_indies: HashMap<TaskId, NodeIndex>,
    pub(crate) dag: Dag<ExecutionNode, ExecutionEdge>,
}

impl ExecutionGraph {
    pub fn new() -> Self {
        ExecutionGraph {
            node_indies: HashMap::new(),
            dag: Dag::new(),
        }
    }

    pub fn build(&mut self, job_graph: &JobGraph) -> Result<(), DagError> {
        let execution_node_indies = self.build_nodes(job_graph);
        self.build_edges(job_graph, execution_node_indies)
    }

    pub fn build_nodes(&mut self, job_graph: &JobGraph) -> HashMap<JobId, Vec<NodeIndex>> {
        let job_dag = &job_graph.dag;

        // HashMap<JobId(u32), Vec<NodeIndex>>
        let mut execution_node_indies = HashMap::new();
        for (_job_id, job_node_index) in &job_graph.job_node_indies {
            let job_node = job_dag.index(*job_node_index);

            for task_number in 0..job_node.parallelism {
                let task_id = TaskId {
                    job_id: job_node.job_id,
                    task_number,
                    num_tasks: job_node.parallelism,
                };
                let execution_node = ExecutionNode {
                    task_id: task_id.clone(),
                    // daemon: job_node.daemon,
                };

                let exec_node_index = self.dag.add_node(execution_node);
                self.node_indies.insert(task_id, exec_node_index);

                execution_node_indies
                    .entry(job_node.job_id)
                    .or_insert(Vec::new())
                    .push(exec_node_index);
            }
        }

        execution_node_indies
    }

    fn build_edges(
        &mut self,
        job_graph: &JobGraph,
        execution_node_indies: HashMap<JobId, Vec<NodeIndex>>,
    ) -> Result<(), DagError> {
        for (job_id, exec_node_indies) in &execution_node_indies {
            for (job_node, job_edge) in job_graph.children(job_id) {
                let child_exec_node_indies = execution_node_indies.get(&job_node.job_id).unwrap();
                match job_edge {
                    JobEdge::Forward => {
                        // build pipeline execution edge

                        if child_exec_node_indies.len() != exec_node_indies.len() {
                            return Err(DagError::JobParallelismNotFound);
                        }

                        for number in 0..exec_node_indies.len() {
                            let node_index = exec_node_indies[number];
                            let child_node_index = child_exec_node_indies[number];
                            self.dag
                                .add_edge(node_index, child_node_index, ExecutionEdge::Memory)
                                .map_err(|_e| DagError::WouldCycle)?;
                        }
                    }
                    JobEdge::ReBalance => {
                        // build cartesian product execution edge
                        for node_index in exec_node_indies {
                            for child_node_index in child_exec_node_indies {
                                self.dag
                                    .add_edge(
                                        *node_index,
                                        *child_node_index,
                                        ExecutionEdge::Network,
                                    )
                                    .map_err(|_e| DagError::WouldCycle)?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
