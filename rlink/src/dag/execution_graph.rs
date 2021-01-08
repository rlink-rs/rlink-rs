use std::collections::HashMap;
use std::ops::Index;

use daggy::{Dag, EdgeIndex, NodeIndex, Walker};

use crate::api::function::InputSplit;
use crate::api::operator::StreamOperatorWrap;
use crate::dag::job_graph::{JobEdge, JobGraph};
use crate::dag::{DagError, Label, TaskId};

#[derive(Copy, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub(crate) enum ExecutionEdge {
    /// Forward
    Memory = 1,
    /// Hash
    Network = 2,
}

impl Label for ExecutionEdge {
    fn get_label(&self) -> String {
        format!("{:?}", self)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct ExecutionNode {
    // pub job_id: u32,
    // pub task_number: u16,
    // /// total number tasks in the chain. same as `parallelism`
    // pub num_tasks: u16,
    pub task_id: TaskId,
    pub input_split: InputSplit,
}

impl Label for ExecutionNode {
    fn get_label(&self) -> String {
        format!(
            "job:{}({}/{})",
            self.task_id.job_id, self.task_id.task_number, self.task_id.num_tasks
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ExecutionGraph {
    pub(crate) dag: Dag<ExecutionNode, ExecutionEdge>,
}

impl ExecutionGraph {
    pub fn new() -> Self {
        ExecutionGraph { dag: Dag::new() }
    }

    // pub fn get_nodes(&self) -> Vec<ExecutionNode> {
    //     self.dag
    //         .raw_nodes()
    //         .iter()
    //         .map(|node| node.weight.clone())
    //         .collect()
    // }

    pub fn build(
        &mut self,
        job_graph: &JobGraph,
        operators: &mut HashMap<u32, &StreamOperatorWrap>,
    ) -> Result<(), DagError> {
        let execution_node_indies = self.build_nodes(job_graph, operators)?;
        self.build_edges(job_graph, execution_node_indies)
    }

    pub fn build_nodes(
        &mut self,
        job_graph: &JobGraph,
        operators: &mut HashMap<u32, &StreamOperatorWrap>,
    ) -> Result<HashMap<u32, Vec<NodeIndex>>, DagError> {
        let job_dag = &job_graph.dag;

        // HashMap<JobId(u32), Vec<NodeIndex>>
        let mut execution_node_indies = HashMap::new();
        for (_job_id, node_index) in &job_graph.job_node_indies {
            let job_node = job_dag.index(*node_index);
            let source_stream_node = &job_node.stream_nodes[0];

            let operator = operators.get_mut(&source_stream_node.id).unwrap();
            if let StreamOperatorWrap::StreamSource(op) = operator {
                let input_splits = op.operator_fn.create_input_splits(job_node.parallelism);
                if input_splits.len() != job_node.parallelism as usize {
                    panic!("")
                }

                for task_number in 0..job_node.parallelism {
                    let execution_node = ExecutionNode {
                        task_id: TaskId {
                            job_id: job_node.job_id,
                            task_number: task_number as u16,
                            num_tasks: job_node.parallelism as u16,
                        },
                        input_split: input_splits[task_number as usize].clone(),
                    };

                    let node_index = self.dag.add_node(execution_node);
                    execution_node_indies
                        .entry(job_node.job_id)
                        .or_insert(Vec::new())
                        .push(node_index);
                }
            }
        }

        Ok(execution_node_indies)
    }

    fn build_edges(
        &mut self,
        job_graph: &JobGraph,
        execution_node_index_map: HashMap<u32, Vec<NodeIndex>>,
    ) -> Result<(), DagError> {
        let job_dag = &job_graph.dag;

        for (job_id, execution_node_indies) in &execution_node_index_map {
            let job_node_index = job_graph.job_node_indies.get(job_id).unwrap();

            let children: Vec<(EdgeIndex, NodeIndex)> =
                job_dag.children(*job_node_index).iter(job_dag).collect();

            for (edge_index, child_node_index) in children {
                let child_job_node = job_dag.index(child_node_index);
                let child_execution_node_indies = execution_node_index_map
                    .get(&child_job_node.job_id)
                    .unwrap();

                let job_edge = job_dag.index(edge_index);
                match job_edge {
                    JobEdge::Forward => {
                        // build pipeline execution edge
                        for number in 0..execution_node_indies.len() {
                            let node_index = execution_node_indies[number];
                            let child_node_index = child_execution_node_indies[number];
                            self.dag
                                .add_edge(node_index, child_node_index, ExecutionEdge::Memory)
                                .unwrap();
                        }
                    }
                    JobEdge::Hash => {
                        // build cartesian product execution edge
                        for node_index in execution_node_indies {
                            for child_node_index in child_execution_node_indies {
                                self.dag
                                    .add_edge(
                                        *node_index,
                                        *child_node_index,
                                        ExecutionEdge::Network,
                                    )
                                    .unwrap();
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
