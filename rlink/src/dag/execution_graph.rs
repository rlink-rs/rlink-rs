use std::collections::HashMap;
use std::ops::Index;

use daggy::{Dag, EdgeIndex, NodeIndex, Walker};

use crate::api::function::InputSplit;
use crate::api::operator::StreamOperatorWrap;
use crate::api::runtime::{JobId, OperatorId};
use crate::dag::job_graph::{JobEdge, JobGraph};
use crate::dag::stream_graph::StreamNode;
use crate::dag::utils::JsonDag;
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
    pub task_id: TaskId,
    pub stream_nodes: Vec<StreamNode>,
    pub input_split: InputSplit,
}

impl Label for ExecutionNode {
    fn get_label(&self) -> String {
        format!(
            "{:?}({}/{})",
            self.task_id.job_id, self.task_id.task_number, self.task_id.num_tasks
        )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ExecutionGraph {
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

    pub(crate) fn get_parents(
        &self,
        task_id: &TaskId,
    ) -> Option<Vec<(ExecutionNode, ExecutionEdge)>> {
        self.node_indies.get(task_id).map(|node_index| {
            let job_nodes: Vec<(ExecutionNode, ExecutionEdge)> = self
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

    pub(crate) fn get_children(
        &self,
        task_id: &TaskId,
    ) -> Option<Vec<(ExecutionNode, ExecutionEdge)>> {
        self.node_indies.get(&task_id).map(|node_index| {
            let job_nodes: Vec<(ExecutionNode, ExecutionEdge)> = self
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

    pub fn build(
        &mut self,
        job_graph: &JobGraph,
        operators: &mut HashMap<OperatorId, &StreamOperatorWrap>,
    ) -> Result<(), DagError> {
        let execution_node_indies = self.build_nodes(job_graph, operators)?;
        self.build_edges(job_graph, execution_node_indies)
    }

    pub fn build_nodes(
        &mut self,
        job_graph: &JobGraph,
        operators: &mut HashMap<OperatorId, &StreamOperatorWrap>,
    ) -> Result<HashMap<JobId, Vec<NodeIndex>>, DagError> {
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
                    return Err(DagError::IllegalInputSplitSize(format!(
                        "{}'s parallelism = {}, but input_splits size = {}",
                        op.operator_fn.get_name(),
                        job_node.parallelism,
                        input_splits.len(),
                    )));
                }

                for task_number in 0..job_node.parallelism {
                    let task_id = TaskId {
                        job_id: job_node.job_id,
                        task_number: task_number as u16,
                        num_tasks: job_node.parallelism as u16,
                    };
                    let execution_node = ExecutionNode {
                        task_id: task_id.clone(),
                        stream_nodes: job_node.stream_nodes.clone(),
                        input_split: input_splits[task_number as usize].clone(),
                    };

                    let node_index = self.dag.add_node(execution_node);
                    self.node_indies.insert(task_id, node_index);

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
        execution_node_index_map: HashMap<JobId, Vec<NodeIndex>>,
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
                    JobEdge::ReBalance => {
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

    pub(crate) fn to_string(&self) -> String {
        JsonDag::dag_json(&self.dag).to_string()
    }
}
