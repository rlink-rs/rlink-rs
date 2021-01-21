use std::cmp::max;
use std::collections::HashMap;
use std::ops::Index;

use daggy::{Dag, NodeIndex, Walker};
use serde::Serialize;

use crate::dag::Label;

pub(crate) fn get_nodes<N, E>(dag: &Dag<N, E>) -> Vec<N>
where
    N: Clone,
{
    dag.raw_nodes()
        .iter()
        .map(|node| node.weight.clone())
        .collect()
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct JsonNode<N>
where
    N: Serialize,
{
    id: String,
    label: String,
    #[serde(rename = "type")]
    ty: String,
    detail: Option<N>,
    dept: isize,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct JsonEdge<E>
where
    E: Serialize,
{
    /// source JsonNode id
    source: String,
    /// target JsonNode id
    target: String,
    label: String,
    detail: Option<E>,
    dept: isize,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct JsonDag<N, E>
where
    N: Clone + Label + Serialize,
    E: Clone + Label + Serialize,
{
    nodes: Vec<JsonNode<N>>,
    edges: Vec<JsonEdge<E>>,
}

impl<N, E> JsonDag<N, E>
where
    N: Clone + Label + Serialize,
    E: Clone + Label + Serialize,
{
    fn get_node_type(dag: &Dag<N, E>, node_index: NodeIndex) -> &str {
        let parent_count = dag.parents(node_index).iter(dag).count();
        if parent_count == 0 {
            "begin"
        } else {
            let child_count = dag.children(node_index).iter(dag).count();
            if child_count == 0 {
                "end"
            } else {
                ""
            }
        }
    }

    fn crate_json_node(dag: &Dag<N, E>, node_index: NodeIndex) -> JsonNode<N> {
        let n = dag.index(node_index);
        let label = n.get_label();
        let id = node_index.index().to_string();
        let ty = JsonDag::get_node_type(dag, node_index);

        JsonNode {
            id,
            label,
            ty: ty.to_string(),
            detail: Some(n.clone()),
            dept: -1,
        }
    }

    pub(crate) fn dag_json(dag: &Dag<N, E>) -> Self {
        let mut node_map = HashMap::new();
        let mut edges = Vec::new();

        for edge in dag.raw_edges() {
            let source_json_node = JsonDag::crate_json_node(dag, edge.source());
            let target_json_node = JsonDag::crate_json_node(dag, edge.target());

            let json_edge = {
                let label = edge.weight.get_label();
                JsonEdge {
                    source: source_json_node.id.clone(),
                    target: target_json_node.id.clone(),
                    label,
                    detail: Some(edge.weight.clone()),
                    dept: -1,
                }
            };

            node_map.insert(source_json_node.id.clone(), source_json_node);
            node_map.insert(target_json_node.id.clone(), target_json_node);

            edges.push(json_edge);
        }

        let nodes = node_map.into_iter().map(|(_, node)| node).collect();

        let mut json_dag = JsonDag { nodes, edges };
        json_dag.sort_nodes();
        json_dag.sort_edge();

        json_dag
    }

    fn sort_nodes(&mut self) {
        let mut node_indies = HashMap::new();
        for i in 0..self.nodes.len() {
            let node = self.nodes.get(i).unwrap();
            node_indies.insert(node.id.clone(), i);
        }

        let root_source_ids: Vec<String> = self
            .nodes
            .iter()
            .filter(|node| {
                self.edges
                    .iter()
                    .find(|edge| edge.target.eq(&node.id))
                    .is_none()
            })
            .map(|node| node.id.clone())
            .collect();

        root_source_ids.into_iter().for_each(|node_id| {
            let index = node_indies.get(&node_id).unwrap();
            self.nodes.get_mut(*index).unwrap().dept = 0;
        });

        self.dept_build(0, &node_indies);

        self.nodes.sort_by_key(|node| node.dept);
    }

    fn dept_build(&mut self, parent_dept: isize, node_indies: &HashMap<String, usize>) {
        for i in 0..self.edges.len() {
            let (source_index, target_index) = {
                let edge = self.edges.get(i).unwrap();
                let source_index = node_indies.get(&edge.source).unwrap();
                let target_index = node_indies.get(&edge.target).unwrap();
                (*source_index, *target_index)
            };

            let source_dept = self.nodes.get(source_index).unwrap().dept;
            if source_dept == parent_dept {
                let target = self.nodes.get_mut(target_index).unwrap();

                let next_dept = parent_dept + 1;
                target.dept = if target.dept == -1 {
                    next_dept
                } else {
                    max(target.dept, next_dept)
                };

                self.dept_build(next_dept, node_indies);
            }
        }
    }

    fn sort_edge(&mut self) {
        let mut edge_index_map = HashMap::new();
        for i in 0..self.edges.len() {
            let edge = self.edges.get(i).unwrap();
            let indies = edge_index_map.entry(edge.source.clone()).or_insert(vec![]);
            indies.push(i);
        }

        for i in 0..self.nodes.len() {
            let (edge_index, source_dept) = {
                let node = self.nodes.get(i).unwrap();
                let edge_index = edge_index_map.get(&node.id).map(|x| x.clone());
                (edge_index, node.dept)
            };
            edge_index.map(|edge_indies| {
                edge_indies.into_iter().for_each(|edge_index| {
                    self.edges.get_mut(edge_index).unwrap().dept = source_dept;
                });
            });
        }

        self.edges.sort_by_key(|edge| edge.dept);
    }

    pub(crate) fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap_or("".to_string())
    }
}
