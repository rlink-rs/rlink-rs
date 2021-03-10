use std::collections::HashMap;
use std::ops::Index;

use daggy::{Dag, NodeIndex};
use serde::Serialize;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct JsonNode<N>
where
    N: Serialize,
{
    id: String,
    detail: N,
}

impl<N> JsonNode<N>
where
    N: Serialize,
{
    pub fn id(&self) -> &str {
        &self.id
    }

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
    detail: E,
}

impl<E> JsonEdge<E>
where
    E: Serialize,
{
    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    pub fn detail(&self) -> &E {
        &self.detail
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct JsonDag<N, E>
where
    N: Clone + Serialize,
    E: Clone + Serialize,
{
    nodes: Vec<JsonNode<N>>,
    edges: Vec<JsonEdge<E>>,
}

impl<'a, N, E> From<&'a Dag<N, E>> for JsonDag<N, E>
where
    N: Clone + Serialize,
    E: Clone + Serialize,
{
    fn from(dag: &'a Dag<N, E, u32>) -> Self {
        let mut node_map = HashMap::new();
        let mut edges = Vec::new();

        if dag.raw_edges().len() > 0 {
            for edge in dag.raw_edges() {
                let source_json_node = JsonDag::crate_json_node(dag, edge.source());
                let target_json_node = JsonDag::crate_json_node(dag, edge.target());

                let json_edge = {
                    JsonEdge {
                        source: source_json_node.id.clone(),
                        target: target_json_node.id.clone(),
                        detail: edge.weight.clone(),
                    }
                };

                node_map.insert(source_json_node.id.clone(), source_json_node);
                node_map.insert(target_json_node.id.clone(), target_json_node);

                edges.push(json_edge);
            }
        } else {
            // no edges
            let mut n = 0;
            dag.raw_nodes().iter().for_each(|node| {
                let json_node = JsonNode {
                    id: n.to_string(),
                    detail: node.weight.clone(),
                };
                node_map.insert(json_node.id.clone(), json_node);

                n += 1;
            });
        }

        let nodes = node_map.into_iter().map(|(_, node)| node).collect();

        JsonDag { nodes, edges }
    }
}

impl<N, E> JsonDag<N, E>
where
    N: Clone + Serialize,
    E: Clone + Serialize,
{
    fn crate_json_node(dag: &Dag<N, E>, node_index: NodeIndex) -> JsonNode<N> {
        let n = dag.index(node_index);
        let id = node_index.index().to_string();

        JsonNode {
            id,
            detail: n.clone(),
        }
    }

    pub(crate) fn get_node(&self, id: &str) -> Option<&JsonNode<N>> {
        self.nodes.iter().find(|node| node.id.eq(id))
    }

    pub(crate) fn parents(&self, parent_node_id: &str) -> Vec<(&JsonNode<N>, &JsonEdge<E>)> {
        self.gets(parent_node_id, true)
    }

    pub(crate) fn children(&self, child_node_id: &str) -> Vec<(&JsonNode<N>, &JsonEdge<E>)> {
        self.gets(child_node_id, false)
    }

    fn gets(&self, node_id: &str, parent: bool) -> Vec<(&JsonNode<N>, &JsonEdge<E>)> {
        self.edges()
            .iter()
            .filter_map(|edge| {
                let node = if parent {
                    if edge.target().eq(node_id) {
                        Some(self.get_node(edge.source()).unwrap())
                    } else {
                        None
                    }
                } else {
                    if edge.source().eq(node_id) {
                        Some(self.get_node(edge.target()).unwrap())
                    } else {
                        None
                    }
                };
                node.map(|node| (node, edge))
            })
            .collect()
    }
}

impl<N, E> JsonDag<N, E>
where
    N: Clone + Serialize,
    E: Clone + Serialize,
{
    pub fn nodes(&self) -> &Vec<JsonNode<N>> {
        &self.nodes
    }

    pub fn edges(&self) -> &Vec<JsonEdge<E>> {
        &self.edges
    }
}
