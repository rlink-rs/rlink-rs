use daggy::Dag;

pub(crate) fn get_nodes<N, E>(dag: &Dag<N, E>) -> Vec<N>
where
    N: Clone,
{
    dag.raw_nodes()
        .iter()
        .map(|node| node.weight.clone())
        .collect()
}
