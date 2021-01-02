use crate::dag::{StreamEdge, StreamGraph, StreamNode};
use daggy::Dag;

#[derive(Debug)]
pub(crate) struct JobGraph {
    stream_graph: StreamGraph,

    dag: Dag<StreamNode, StreamEdge>,
}

impl JobGraph {
    pub fn new(stream_graph: StreamGraph) -> Self {
        JobGraph {
            stream_graph,
            dag: Dag::new(),
        }
    }
}
