use crate::api::operator::{StreamOperatorWrap, TStreamOperator, DEFAULT_PARALLELISM};
use crate::graph::{ChainEdge, GraphNode, JobGraph, OperatorChain};
use std::collections::HashMap;

pub fn build_job_graph(operators: Vec<StreamOperatorWrap>) -> JobGraph {
    if operators.len() == 0 {
        panic!("No Operator");
    }

    let mut operator_chains = build_source_plan(&operators);

    // set current chain's `follower_edge` by next chain's `dependency_edge`
    for index in 0..operator_chains.len() - 1 {
        let follower_chain = operator_chains.get(index + 1).unwrap().clone();
        let chain = operator_chains.get_mut(index).unwrap();

        match follower_chain.dependency_edge {
            ChainEdge::InSameTask => chain.follower_edge = ChainEdge::InSameTask,
            ChainEdge::CrossTask => chain.follower_edge = ChainEdge::CrossTask,
        }

        chain.follower_chain_id = follower_chain.chain_id;
        chain.follower_parallelism = follower_chain.parallelism;
    }
    // set current chain's `dependency_parallelism` by front chain's `parallelism`
    for index in 1..operator_chains.len() {
        let dependency_chain = operator_chains.get(index - 1).unwrap().clone();
        let chain = operator_chains.get_mut(index).unwrap();

        chain.dependency_parallelism = dependency_chain.parallelism;
    }

    let mut chain_map = HashMap::new();
    for operator_chain in operator_chains {
        chain_map.insert(operator_chain.chain_id, operator_chain.clone());
    }
    JobGraph {
        chain_map,
        chain_groups: vec![],
        operators,
    }
}

fn build_source_plan(operators: &Vec<StreamOperatorWrap>) -> Vec<OperatorChain> {
    let source_operator_chain = build_source_plan0(operators);
    let step_index = get_step_index(&source_operator_chain);

    let next_operator = operators.get(step_index);
    if next_operator.is_none() {
        return vec![source_operator_chain];
    }
    let next_operator = next_operator.unwrap();

    let dependency_chain_id = source_operator_chain.chain_id;
    let next_operator_chains = if next_operator.is_window() || next_operator.is_reduce() {
        build_reduce_plan(&operators, step_index, dependency_chain_id)
    } else if next_operator.is_map() || next_operator.is_filter() {
        build_map_filter_plan(operators, step_index, dependency_chain_id, false)
    } else if next_operator.is_sink() {
        build_sink_plan(operators, step_index, dependency_chain_id, false)
    } else {
        panic!("Not Supported Operator")
    };

    let mut operator_chains = vec![source_operator_chain];
    operator_chains.extend(next_operator_chains);
    operator_chains
}

fn build_source_plan0(operators: &Vec<StreamOperatorWrap>) -> OperatorChain {
    let first_operator = operators.get(0).unwrap();
    if !first_operator.is_source() {
        panic!("the Operator must start with `Source` operator");
    }

    let parallelism = first_operator.get_parallelism();
    if parallelism == DEFAULT_PARALLELISM {
        panic!("Operator `Source` must be set the `parallelism`");
    }

    let mut nodes = Vec::new();
    for index in 0..operators.len() {
        let operator = operators.get(index).unwrap();

        if operator.get_parallelism() != parallelism
            && operator.get_parallelism() != DEFAULT_PARALLELISM
        {
            break;
        }

        if operator.is_window() || operator.is_reduce() {
            break;
        }

        let graph_node = GraphNode {
            name: operator.get_operator_name().to_string(),
            node_id: operator.get_operator_id(),
            parent_node_id: operator.get_parent_operator_id(),
            parallelism: operator.get_parallelism(),
            operator_index: index as u32,
        };

        nodes.push(graph_node);
    }

    OperatorChain {
        chain_id: 1,
        dependency_chain_id: 0,
        follower_chain_id: 0,
        // ignore
        dependency_edge: ChainEdge::CrossTask,
        // ignore
        follower_edge: ChainEdge::InSameTask,
        parallelism,
        dependency_parallelism: 0,
        follower_parallelism: 0,
        nodes,
    }
}

fn build_reduce_plan(
    operators: &Vec<StreamOperatorWrap>,
    start_index: usize,
    dependency_chain_id: u32,
) -> Vec<OperatorChain> {
    let first_operator = operators.get(start_index).unwrap();
    if first_operator.is_window() {
        build_window_reduce_plan(operators, start_index, dependency_chain_id)
    } else if first_operator.is_reduce() {
        panic!("the Operator `Reduce` without `Window` are not supported");
    } else {
        panic!("the Operator must start with `Window` or `Reduce` operator");
    }
}

fn build_window_reduce_plan(
    operators: &Vec<StreamOperatorWrap>,
    start_index: usize,
    dependency_chain_id: u32,
) -> Vec<OperatorChain> {
    let window_reduce_operator_chain =
        build_window_reduce_plan0(operators, start_index, dependency_chain_id);
    let step_index = get_step_index(&window_reduce_operator_chain);

    let next_operator = operators.get(step_index).unwrap();
    let dependency_chain_id = window_reduce_operator_chain.chain_id;
    let next_operator_chains = if next_operator.is_map() || next_operator.is_filter() {
        build_map_filter_plan(operators, step_index, dependency_chain_id, true)
    } else if next_operator.is_sink() {
        build_sink_plan(operators, step_index, dependency_chain_id, true)
    } else {
        panic!("Not Supported Operator")
    };

    let mut operator_chains = vec![window_reduce_operator_chain];
    operator_chains.extend(next_operator_chains);
    operator_chains
}

fn build_window_reduce_plan0(
    operators: &Vec<StreamOperatorWrap>,
    start_index: usize,
    dependency_chain_id: u32,
) -> OperatorChain {
    let first_operator = operators.get(start_index).unwrap();
    if !first_operator.is_window() {
        panic!("the Operator must start with `Window` operator");
    }

    let reduce_operator = operators
        .iter()
        .skip(start_index - 1)
        .find(|op| op.is_reduce())
        .expect("No `Reduce` Operator under `Window` mode");

    let parallelism = reduce_operator.get_parallelism();
    if parallelism == DEFAULT_PARALLELISM {
        panic!("Operator `Reduce` must be set the `parallelism`");
    }

    let mut nodes = Vec::new();
    for index in start_index..operators.len() {
        let operator = operators.get(index).unwrap();

        if operator.is_window() {
            if operator.get_parallelism() != DEFAULT_PARALLELISM {
                panic!("Operator `Window` must have the same `parallelism` as `Reduce`")
            }
        } else {
            if operator.get_parallelism() != parallelism
                && operator.get_parallelism() != DEFAULT_PARALLELISM
            {
                panic!(
                    "Operator between `Window` and `Reduce` are not supported custom `parallelism`"
                );
            }
        }

        let graph_node = GraphNode {
            name: operator.get_operator_name().to_string(),
            node_id: operator.get_operator_id(),
            parent_node_id: operator.get_parent_operator_id(),
            parallelism: operator.get_parallelism(),
            operator_index: index as u32,
        };

        nodes.push(graph_node);

        // chain can only contain Operators between `Window` and `Reduce` in `Window` mode
        if operator.is_reduce() {
            break;
        }
    }

    OperatorChain {
        chain_id: dependency_chain_id + 1,
        dependency_chain_id,
        follower_chain_id: 0,
        dependency_edge: ChainEdge::CrossTask,
        follower_edge: ChainEdge::InSameTask,
        parallelism,
        dependency_parallelism: 0,
        follower_parallelism: 0,
        nodes,
    }
}

fn build_map_filter_plan(
    operators: &Vec<StreamOperatorWrap>,
    start_index: usize,
    dependency_chain_id: u32,
    dependency_window: bool,
) -> Vec<OperatorChain> {
    let map_filter_operator_chain = build_map_filter_plan0(
        operators,
        start_index,
        dependency_chain_id,
        dependency_window,
    );

    if check_is_end_chain(&map_filter_operator_chain, operators) {
        return vec![map_filter_operator_chain];
    }

    let step_index = get_step_index(&map_filter_operator_chain);

    let next_operator = operators.get(step_index).unwrap();
    let dependency_chain_id = map_filter_operator_chain.chain_id;
    let next_operator_chains = if next_operator.is_window() || next_operator.is_reduce() {
        build_reduce_plan(&operators, step_index, dependency_chain_id)
    } else if next_operator.is_sink() {
        build_sink_plan(
            operators,
            step_index,
            dependency_chain_id,
            dependency_window,
        )
    } else if next_operator.is_map() || next_operator.is_filter() {
        // build_map_filter_plan(operators, step_index, map_filter_operator_chain.chain_id);
        panic!("Operator `Map` or `Filter` after `Map` or `Filter` art not supported")
    } else {
        panic!("Not Supported Operator")
    };

    let mut operator_chains = vec![map_filter_operator_chain];
    operator_chains.extend(next_operator_chains);
    operator_chains
}

fn build_map_filter_plan0(
    operators: &Vec<StreamOperatorWrap>,
    start_index: usize,
    dependency_chain_id: u32,
    dependency_window: bool,
) -> OperatorChain {
    let first_operator = operators.get(start_index).unwrap();
    if !first_operator.is_map() && !first_operator.is_filter() {
        panic!("the Operator must start with `Map` or `Filter` operator");
    }

    let parallelism = first_operator.get_parallelism();
    if !dependency_window && parallelism == DEFAULT_PARALLELISM {
        panic!("Operator `Map` or `Filter` as first Node must be set the `parallelism`");
    }

    let mut nodes = Vec::new();
    for index in start_index..operators.len() {
        let operator = operators.get(index).unwrap();

        if operator.get_parallelism() != parallelism
            && operator.get_parallelism() != DEFAULT_PARALLELISM
        {
            break;
        }

        if operator.is_window() || operator.is_reduce() {
            break;
        }

        let graph_node = GraphNode {
            name: operator.get_operator_name().to_string(),
            node_id: operator.get_operator_id(),
            parent_node_id: operator.get_parent_operator_id(),
            parallelism: operator.get_parallelism(),
            operator_index: index as u32,
        };

        nodes.push(graph_node);
    }

    let dependency_edge = if dependency_window {
        ChainEdge::InSameTask
    } else {
        ChainEdge::CrossTask
    };

    OperatorChain {
        chain_id: dependency_chain_id + 1,
        dependency_chain_id,
        follower_chain_id: 0,
        dependency_edge,
        follower_edge: ChainEdge::InSameTask,
        parallelism,
        dependency_parallelism: 0,
        follower_parallelism: 0,
        nodes,
    }
}

fn build_sink_plan(
    operators: &Vec<StreamOperatorWrap>,
    start_index: usize,
    dependency_chain_id: u32,
    dependency_window: bool,
) -> Vec<OperatorChain> {
    let first_operator = operators.get(start_index).unwrap();
    if !first_operator.is_sink() {
        panic!("the Operator must start with `Sink` operator");
    }

    if start_index != operators.len() - 1 {
        panic!("the Operator `Sink` must be the latest Operator");
    }

    let operator = first_operator;
    let graph_node = GraphNode {
        name: operator.get_operator_name().to_string(),
        node_id: operator.get_operator_id(),
        parent_node_id: operator.get_parent_operator_id(),
        parallelism: operator.get_parallelism(),
        operator_index: start_index as u32,
    };

    let dependency_edge = if dependency_window {
        ChainEdge::InSameTask
    } else {
        ChainEdge::CrossTask
    };

    vec![OperatorChain {
        chain_id: dependency_chain_id + 1,
        dependency_chain_id,
        follower_chain_id: 0,
        dependency_edge,
        follower_edge: ChainEdge::InSameTask,
        parallelism: operator.get_parallelism(),
        dependency_parallelism: 0,
        follower_parallelism: 0,
        nodes: vec![graph_node],
    }]
}

#[inline]
fn get_latest_operator_index(operator_chain: &OperatorChain) -> usize {
    let len = operator_chain.nodes.len();
    operator_chain.nodes[len - 1].operator_index as usize
}

fn get_step_index(operator_chain: &OperatorChain) -> usize {
    // let len = operator_chain.nodes.len();
    // let latest_node = operator_chain.nodes.get(len - 1).unwrap();
    // (latest_node.operator_index + 1) as usize

    get_latest_operator_index(operator_chain) + 1
}

fn check_is_end_chain(operator_chain: &OperatorChain, operators: &Vec<StreamOperatorWrap>) -> bool {
    let latest_operator_index = get_latest_operator_index(operator_chain);
    operators.get(latest_operator_index).unwrap().is_sink()
}
