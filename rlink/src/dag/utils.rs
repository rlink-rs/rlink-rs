use std::collections::HashMap;
use std::ops::Index;

use daggy::{Dag, NodeIndex, Walker};
use serde::Serialize;

use crate::dag::Label;

// pub(crate) fn get_nodes<N, E>(dag: &Dag<N, E>) -> Vec<N>
// where
//     N: Clone,
// {
//     dag.raw_nodes()
//         .iter()
//         .map(|node| node.weight.clone())
//         .collect()
// }

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct JsonNode<N>
where
    N: Serialize,
{
    id: String,
    label: String,
    #[serde(rename = "type")]
    ty: String,
    detail: N,
    dept: isize,
}

impl<N> JsonNode<N>
where
    N: Serialize,
{
    pub fn detail(&self) -> &N {
        &self.detail
    }
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
    detail: E,
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

impl<'a, N, E> From<&'a Dag<N, E>> for JsonDag<N, E>
where
    N: Clone + Label + Serialize,
    E: Clone + Label + Serialize,
{
    fn from(dag: &'a Dag<N, E, u32>) -> Self {
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
                    detail: edge.weight.clone(),
                }
            };

            node_map.insert(source_json_node.id.clone(), source_json_node);
            node_map.insert(target_json_node.id.clone(), target_json_node);

            edges.push(json_edge);
        }

        let nodes = node_map.into_iter().map(|(_, node)| node).collect();

        JsonDag { nodes, edges }
    }
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
            detail: n.clone(),
            dept: -1,
        }
    }

    pub(crate) fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap_or("".to_string())
    }
}

impl<N, E> JsonDag<N, E>
where
    N: Clone + Label + Serialize,
    E: Clone + Label + Serialize,
{
    pub fn nodes(&self) -> &Vec<JsonNode<N>> {
        &self.nodes
    }
    // pub fn edges(&self) -> &Vec<JsonEdge<E>> {
    //     &self.edges
    // }
}
