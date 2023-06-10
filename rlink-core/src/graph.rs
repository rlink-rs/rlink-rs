use std::collections::HashMap;
use std::ops::{Deref, Index};

use serde::Serialize;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct Node<Id, N>
where
    Id: Clone + Serialize,
    N: Serialize,
{
    id: Id,
    detail: N,
}

impl<Id, N> Node<Id, N>
where
    Id: Clone + Serialize,
    N: Serialize,
{
    pub fn id(&self) -> &Id {
        &self.id
    }
}

impl<Id, N> Deref for Node<Id, N>
where
    Id: Clone + Serialize,
    N: Serialize,
{
    type Target = N;

    fn deref(&self) -> &Self::Target {
        &self.detail
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct Edge<Id, E>
where
    Id: Clone + Serialize,
    E: Serialize,
{
    source: Id,
    target: Id,
    detail: E,
}

impl<Id, E> Edge<Id, E>
where
    Id: Clone + Serialize,
    E: Serialize,
{
    pub fn source(&self) -> &Id {
        &self.source
    }

    pub fn target(&self) -> &Id {
        &self.target
    }
}

impl<Id, E> Deref for Edge<Id, E>
where
    Id: Clone + Serialize,
    E: Serialize,
{
    type Target = E;

    fn deref(&self) -> &Self::Target {
        &self.detail
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct Dag<Id, N, E>
where
    Id: Clone + Serialize,
    N: Clone + Serialize,
    E: Clone + Serialize,
{
    nodes: Vec<Node<Id, N>>,
    edges: Vec<Edge<Id, E>>,
}

impl<Id, N, E> Dag<Id, N, E>
where
    Id: Clone + Serialize,
    N: Clone + Serialize,
    E: Clone + Serialize,
{
    pub(crate) fn get_node(&self, id: &str) -> Option<&Node<Id, N>> {
        self.nodes.iter().find(|node| node.id.eq(id))
    }

    pub(crate) fn parents(&self, parent_node_id: &str) -> Vec<(&Node<Id, N>, &Edge<Id, E>)> {
        self.gets(parent_node_id, true)
    }

    pub(crate) fn children(&self, child_node_id: &str) -> Vec<(&Node<Id, N>, &Edge<Id, E>)> {
        self.gets(child_node_id, false)
    }

    fn gets(&self, node_id: &str, parent: bool) -> Vec<(&Node<Id, N>, &Edge<Id, E>)> {
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

impl<Id, N, E> Dag<Id, N, E>
where
    Id: Clone + Serialize,
    N: Clone + Serialize,
    E: Clone + Serialize,
{
    pub fn nodes(&self) -> &Vec<Node<Id, N>> {
        &self.nodes
    }

    pub fn edges(&self) -> &Vec<Edge<Id, E>> {
        &self.edges
    }
}
