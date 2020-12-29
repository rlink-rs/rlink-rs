use std::collections::HashMap;

use crate::api::function::{InputFormat, OutputFormat};
use crate::api::operator::{FunctionCreator, StreamOperatorWrap, TStreamOperator};
use crate::graph::{ChainEdge, GraphNode, JobGraph, OperatorChain};
use crate::runtime::worker::io::mem_channel_input::MemChannelInputFormat;
use crate::runtime::worker::io::mem_channel_output::MemChannelOutputFormat;
use crate::runtime::worker::io::net_channel_input::NetChannelInputFormat;
use crate::runtime::worker::io::net_channel_output::NetChannelOutputFormat;
use crate::storage::metadata::MetadataLoader;

pub fn revise_logic_graph(mut job_graph: JobGraph, metadata_loader: MetadataLoader) -> JobGraph {
    let job_chain = job_graph.chain_map.clone();
    let mut revise_job_chain = job_chain.clone();

    let mut additional_index = 10000;
    for (chain_id, chain) in job_chain {
        // try to add source
        let first_node = &chain.nodes[0];
        let first_operator = &job_graph.operators[first_node.operator_index as usize];

        if !first_operator.is_source() {
            let node_id = additional_index;
            additional_index += 1;
            let parent_node_id = 0;

            let stream_source = create_source(
                &chain.dependency_edge,
                node_id,
                parent_node_id,
                chain.parallelism,
                chain_id,
                chain.dependency_chain_id,
                metadata_loader.clone(),
            );
            let name = stream_source.get_operator_name().to_string();
            job_graph.operators.push(stream_source);

            revise_job_chain.get_mut(&chain_id).unwrap().nodes[0].parent_node_id = node_id;
            let node = GraphNode {
                name,
                node_id,
                parent_node_id,
                parallelism: chain.parallelism,
                operator_index: (job_graph.operators.len() - 1) as u32,
            };
            revise_job_chain
                .get_mut(&chain_id)
                .unwrap()
                .nodes
                .insert(0, node);
        }

        // try to add sink
        let latest_node = &chain.nodes[chain.nodes.len() - 1];
        let latest_operator = &job_graph.operators[latest_node.operator_index as usize];

        if !latest_operator.is_sink() {
            let next_chains = get_next_chains(&revise_job_chain, chain_id.clone());
            let next_chain = next_chains.get(0).expect("next chain not found");
            let next_chain_parallelism = next_chain.parallelism;

            let node_id = additional_index;
            additional_index += 1;
            let parent_node_id = latest_node.node_id;

            let stream_sink = create_sink(
                &chain.follower_edge,
                node_id,
                parent_node_id,
                chain_id,
                next_chain_parallelism,
            );
            let name = stream_sink.get_operator_name().to_string();
            job_graph.operators.push(stream_sink);

            let node = GraphNode {
                name,
                node_id,
                parent_node_id,
                parallelism: 0,
                operator_index: (job_graph.operators.len() - 1) as u32,
            };
            revise_job_chain
                .get_mut(&chain_id)
                .unwrap()
                .nodes
                .push(node);
        }
    }

    JobGraph {
        chain_map: revise_job_chain,
        chain_groups: vec![],
        operators: job_graph.operators,
    }
}

fn create_source(
    dependency_edge: &ChainEdge,
    id: u32,
    parent_id: u32,
    chain_parallelism: u32,
    chain_id: u32,
    dependency_chain_id: u32,
    metadata_loader: MetadataLoader,
) -> StreamOperatorWrap {
    match dependency_edge {
        ChainEdge::InSameTask => create_mem_source(
            id,
            parent_id,
            chain_parallelism,
            chain_id,
            dependency_chain_id,
        ),
        ChainEdge::CrossTask => create_net_source(
            id,
            parent_id,
            chain_parallelism,
            chain_id,
            dependency_chain_id,
            metadata_loader,
        ),
    }
}

fn create_net_source(
    id: u32,
    parent_id: u32,
    chain_parallelism: u32,
    chain_id: u32,
    dependency_chain_id: u32,
    metadata_loader: MetadataLoader,
) -> StreamOperatorWrap {
    let input_func = NetChannelInputFormat::new(chain_id, dependency_chain_id, metadata_loader);
    let source_func: Box<dyn InputFormat> = Box::new(input_func);

    let stream_source = StreamOperatorWrap::new_source(
        id,
        parent_id,
        chain_parallelism,
        FunctionCreator::System,
        source_func,
    );
    stream_source
}

fn create_mem_source(
    id: u32,
    parent_id: u32,
    chain_parallelism: u32,
    chain_id: u32,
    dependency_chain_id: u32,
) -> StreamOperatorWrap {
    let input_func = MemChannelInputFormat::new(chain_id, dependency_chain_id);
    let source_func: Box<dyn InputFormat> = Box::new(input_func);

    let stream_source = StreamOperatorWrap::new_source(
        id,
        parent_id,
        chain_parallelism,
        FunctionCreator::System,
        source_func,
    );
    stream_source
}

fn create_sink(
    follower_edge: &ChainEdge,
    id: u32,
    parent_id: u32,
    chain_id: u32,
    next_chain_parallelism: u32,
) -> StreamOperatorWrap {
    match follower_edge {
        ChainEdge::InSameTask => create_mem_sink(id, parent_id, chain_id),
        ChainEdge::CrossTask => create_net_sink(id, parent_id, chain_id, next_chain_parallelism),
    }
}

fn create_net_sink(
    id: u32,
    parent_id: u32,
    chain_id: u32,
    next_chain_parallelism: u32,
) -> StreamOperatorWrap {
    let output_func = NetChannelOutputFormat::new(chain_id, next_chain_parallelism);
    let sink_func: Box<dyn OutputFormat> = Box::new(output_func);

    let stream_sink =
        StreamOperatorWrap::new_sink(id, parent_id, FunctionCreator::System, sink_func);
    stream_sink
}

fn create_mem_sink(id: u32, parent_id: u32, chain_id: u32) -> StreamOperatorWrap {
    let output_func = MemChannelOutputFormat::new(chain_id);
    let sink_func: Box<dyn OutputFormat> = Box::new(output_func);

    let stream_sink =
        StreamOperatorWrap::new_sink(id, parent_id, FunctionCreator::System, sink_func);
    stream_sink
}

fn get_next_chains(
    chain_map: &HashMap<u32, OperatorChain>,
    dependency_chain_id: u32,
) -> Vec<OperatorChain> {
    let mut dependency_chains = Vec::new();
    for (_chain_id, chain) in chain_map {
        if chain.dependency_chain_id == dependency_chain_id {
            dependency_chains.push(chain.clone())
        }
    }

    dependency_chains
}
