use std::collections::HashMap;

use crate::graph::{ChainEdge, JobGraph, OperatorChain};

pub fn build_logic_plan_group(job_graph: JobGraph) -> JobGraph {
    let mut chain_groups = Vec::new();
    let mut chain_map = job_graph.chain_map.clone();

    for (_chain_id, chain) in &job_graph.chain_map {
        if let ChainEdge::CrossTask = chain.dependency_edge {
            let mut chain_group = match chain.follower_edge {
                ChainEdge::CrossTask => vec![chain.clone()],
                ChainEdge::InSameTask => build_in_same_task_chains(&job_graph.chain_map, chain),
            };

            // `ChainGroup` must have the same `parallelism`
            // ensure the `InSameTask` chain's number of tasks is aligned
            let parallelism = chain_group[0].parallelism;
            let follower_parallelism = chain_group[chain_group.len() - 1].follower_parallelism;
            for index in 1..chain_group.len() {
                chain_group[index].parallelism = parallelism;
                chain_group[index].follower_parallelism = follower_parallelism;

                chain_map
                    .get_mut(&chain_group[index].chain_id)
                    .unwrap()
                    .parallelism = parallelism;
                chain_map
                    .get_mut(&chain_group[index].chain_id)
                    .unwrap()
                    .follower_parallelism = follower_parallelism;
            }

            chain_groups.push(chain_group)
        }
    }

    JobGraph {
        chain_map,
        chain_groups,
        operators: job_graph.operators,
    }
}

fn build_in_same_task_chains(
    chain_map: &HashMap<u32, OperatorChain>,
    first_chain: &OperatorChain,
) -> Vec<OperatorChain> {
    let mut chains = Vec::new();
    chains.push(first_chain.clone());

    for (_chain_id, chain) in chain_map {
        if chain.dependency_chain_id == first_chain.chain_id {
            let follower_chains = build_in_same_task_chains(chain_map, chain);
            chains.extend(follower_chains);
            return chains;
        }
    }

    chains
}
