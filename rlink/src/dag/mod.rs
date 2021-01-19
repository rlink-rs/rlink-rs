//! DAG builder
//! stream_graph -> job_graph -> execution_graph

use std::borrow::BorrowMut;
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Index;

use daggy::{Dag, NodeIndex, Walker};
use serde::Serialize;
use thiserror::Error;

use crate::api::function::InputSplit;
use crate::api::operator::StreamOperatorWrap;
use crate::api::runtime::{JobId, OperatorId, TaskId};
use crate::dag::execution_graph::{ExecutionEdge, ExecutionGraph, ExecutionNode};
use crate::dag::job_graph::{JobEdge, JobGraph, JobNode};
use crate::dag::physic_graph::PhysicGraph;
use crate::dag::stream_graph::{StreamGraph, StreamNode};

pub(crate) mod execution_graph;
pub(crate) mod job_graph;
pub(crate) mod physic_graph;
pub(crate) mod stream_graph;
pub(crate) mod utils;

pub(crate) use stream_graph::RawStreamGraph;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct TaskInstance {
    pub task_id: TaskId,
    pub stream_nodes: Vec<StreamNode>,
    pub input_split: InputSplit,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct WorkerManagerInstance {
    /// build by self, format `format!("task_manager_{}", index)`
    pub worker_manager_id: String,
    /// task instances
    pub task_instances: Vec<TaskInstance>,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub(crate) enum OperatorType {
    Source,
    FlatMap,
    Filter,
    CoProcess,
    KeyBy,
    Reduce,
    WatermarkAssigner,
    WindowAssigner,
    Sink,
}

impl<'a> From<&'a StreamOperatorWrap> for OperatorType {
    fn from(op: &'a StreamOperatorWrap) -> Self {
        match op {
            StreamOperatorWrap::StreamSource(_) => OperatorType::Source,
            StreamOperatorWrap::StreamFlatMap(_) => OperatorType::FlatMap,
            StreamOperatorWrap::StreamFilter(_) => OperatorType::Filter,
            StreamOperatorWrap::StreamCoProcess(_) => OperatorType::CoProcess,
            StreamOperatorWrap::StreamKeyBy(_) => OperatorType::KeyBy,
            StreamOperatorWrap::StreamReduce(_) => OperatorType::Reduce,
            StreamOperatorWrap::StreamWatermarkAssigner(_) => OperatorType::WatermarkAssigner,
            StreamOperatorWrap::StreamWindowAssigner(_) => OperatorType::WindowAssigner,
            StreamOperatorWrap::StreamSink(_) => OperatorType::Sink,
        }
    }
}

impl std::fmt::Display for OperatorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperatorType::Source => write!(f, "Source"),
            OperatorType::FlatMap => write!(f, "Map"),
            OperatorType::Filter => write!(f, "Filter"),
            OperatorType::CoProcess => write!(f, "CoProcess"),
            OperatorType::KeyBy => write!(f, "KeyBy"),
            OperatorType::Reduce => write!(f, "Reduce"),
            OperatorType::WatermarkAssigner => write!(f, "WatermarkAssigner"),
            OperatorType::WindowAssigner => write!(f, "WindowAssigner"),
            OperatorType::Sink => write!(f, "Sink"),
        }
    }
}

#[derive(Error, Debug)]
pub enum DagError {
    #[error("source not found")]
    SourceNotFound,
    #[error("source not at staring")]
    SourceNotAtStarting,
    #[error("source not at ending")]
    SinkNotAtEnding,
    #[error("the operator is not combine operator")]
    NotCombineOperator,
    #[error("parent operator not found")]
    ParentOperatorNotFound,
    #[error("child not found in a pipeline job")]
    ChildNotFoundInPipeline,
    #[error("multi-children in a pipeline job")]
    MultiChildrenInPipeline,
    #[error("illegal Vec<InputSplit> len. {0}")]
    IllegalInputSplitSize(String),
}

pub(crate) trait Label {
    fn get_label(&self) -> String;
}

#[derive(Clone, Debug)]
pub(crate) struct DagManager {
    stream_graph: StreamGraph,
    job_graph: JobGraph,
    execution_graph: ExecutionGraph,
    physic_graph: PhysicGraph,
}

impl DagManager {
    pub fn new(raw_stream_graph: &RawStreamGraph) -> Self {
        let stream_graph = StreamGraph::new(
            raw_stream_graph.sources.clone(),
            raw_stream_graph.dag.clone(),
        );

        let mut job_graph = JobGraph::new();
        job_graph.build(&stream_graph).unwrap();

        let mut execution_graph = ExecutionGraph::new();
        execution_graph
            .build(&job_graph, raw_stream_graph.get_operators().borrow_mut())
            .unwrap();

        let mut physic_graph = PhysicGraph::new();
        physic_graph.build(&execution_graph);

        DagManager {
            stream_graph,
            job_graph,
            execution_graph,
            physic_graph,
        }
    }

    pub fn stream_graph(&self) -> &StreamGraph {
        &self.stream_graph
    }

    pub fn job_graph(&self) -> &JobGraph {
        &self.job_graph
    }

    pub fn execution_graph(&self) -> &ExecutionGraph {
        &self.execution_graph
    }

    pub fn physic_graph(&self) -> &PhysicGraph {
        &self.physic_graph
    }

    // pub fn get_stream_parents(&self, operator_id: u32) -> Vec<StreamNode> {
    //     let parents = self.stream_graph.get_parents(operator_id);
    //     let parents: Vec<StreamNode> = parents.into_iter().map(|x| x.clone()).collect();
    //     parents
    // }

    pub fn get_stream(&self, operator_id: OperatorId) -> Option<StreamNode> {
        self.stream_graph
            .get_stream_node_by_operator_id(operator_id)
            .map(|stream_node| stream_node.clone())
    }

    #[inline]
    pub fn get_job_node(&self, task_id: &TaskId) -> Option<JobNode> {
        self.job_graph.get_job_node(task_id.job_id)
    }

    #[inline]
    pub(crate) fn get_job_parents(&self, job_id: JobId) -> Vec<(JobNode, JobEdge)> {
        self.job_graph.get_parents(job_id).unwrap_or(vec![])
    }

    #[inline]
    pub(crate) fn get_job_children(&self, job_id: JobId) -> Vec<(JobNode, JobEdge)> {
        self.job_graph.get_children(job_id).unwrap_or(vec![])
    }

    pub fn get_task_parents(&self, task_id: &TaskId) -> Vec<(ExecutionNode, ExecutionEdge)> {
        self.execution_graph.get_parents(task_id).unwrap_or(vec![])
    }

    pub fn get_task_children(&self, task_id: &TaskId) -> Vec<(ExecutionNode, ExecutionEdge)> {
        self.execution_graph.get_children(task_id).unwrap_or(vec![])
    }
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
        let mut edge_indies = HashMap::new();
        for i in 0..self.edges.len() {
            let edge = self.edges.get(i).unwrap();
            edge_indies.insert(edge.source.clone(), i);
        }

        for i in 0..self.nodes.len() {
            let (edge_index, source_dept) = {
                let node = self.nodes.get(i).unwrap();
                let edge_index = edge_indies.get(&node.id).map(|x| *x);
                (edge_index, node.dept)
            };
            edge_index.map(|edge_index| {
                self.edges.get_mut(edge_index).unwrap().dept = source_dept;
            });
        }

        self.edges.sort_by_key(|edge| edge.dept);
    }

    pub(crate) fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap_or("".to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::api;
    use crate::api::data_stream::CoStream;
    use crate::api::data_stream::{TConnectedStreams, TKeyedStream};
    use crate::api::data_stream::{TDataStream, TWindowedStream};
    use crate::api::element::Record;
    use crate::api::env::StreamExecutionEnvironment;
    use crate::api::function::{
        CoProcessFunction, Context, FlatMapFunction, Function, InputFormat, InputSplit,
        InputSplitSource, KeySelectorFunction, OutputFormat, ReduceFunction,
    };
    use crate::api::properties::Properties;
    use crate::api::watermark::{BoundedOutOfOrdernessTimestampExtractor, TimestampAssigner};
    use crate::api::window::SlidingEventTimeWindows;
    use crate::dag::{DagManager, JsonDag};

    #[test]
    pub fn data_stream_test() {
        let mut env = StreamExecutionEnvironment::new("job_name".to_string());

        env.register_source(MyInputFormat::new(), 100)
            .flat_map(MyFlatMapFunction::new())
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(1),
                MyTimestampAssigner::new(),
            ))
            .key_by(MyKeySelectorFunction::new())
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(20),
                None,
            ))
            .reduce(MyReduceFunction::new(), 10)
            .add_sink(MyOutputFormat::new(Properties::new()));

        println!("{:?}", env.stream_manager.stream_graph.borrow().dag);
    }

    #[test]
    pub fn data_stream_connect_test() {
        let mut env = StreamExecutionEnvironment::new("job_name".to_string());

        let ds = env
            .register_source(MyInputFormat::new(), 1)
            .flat_map(MyFlatMapFunction::new())
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(1),
                MyTimestampAssigner::new(),
            ));

        env.register_source(MyInputFormat::new(), 2)
            .flat_map(MyFlatMapFunction::new())
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(1),
                MyTimestampAssigner::new(),
            ))
            .connect(vec![CoStream::from(ds)], MyCoProcessFunction {})
            .key_by(MyKeySelectorFunction::new())
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(20),
                None,
            ))
            .reduce(MyReduceFunction::new(), 3)
            .flat_map(MyFlatMapFunction::new())
            .add_sink(MyOutputFormat::new(Properties::new()));

        let dag_manager = DagManager::new(&env.stream_manager.stream_graph.borrow());
        {
            let dag = &dag_manager.stream_graph().dag;
            println!("{:?}", dag);
            println!(
                "{}",
                serde_json::to_string(&JsonDag::dag_json(dag)).unwrap()
            )
        }
        {
            let dag = &dag_manager.job_graph().dag;
            println!("{:?}", dag);
            println!(
                "{}",
                serde_json::to_string(&JsonDag::dag_json(dag)).unwrap()
            )
        }

        {
            let dag = &dag_manager.execution_graph().dag;
            println!("{:?}", dag);
            println!(
                "{}",
                serde_json::to_string(&JsonDag::dag_json(dag)).unwrap()
            )
        }

        println!("{:?}", &dag_manager.physic_graph());
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct MyInputFormat {}

    impl MyInputFormat {
        pub fn new() -> Self {
            MyInputFormat {}
        }
    }

    impl InputSplitSource for MyInputFormat {}

    impl Function for MyInputFormat {
        fn get_name(&self) -> &str {
            "MyInputFormat"
        }
    }

    impl InputFormat for MyInputFormat {
        fn open(&mut self, _input_split: InputSplit, _context: &Context) -> api::Result<()> {
            Ok(())
        }

        fn reached_end(&self) -> bool {
            false
        }

        fn next_record(&mut self) -> Option<Record> {
            None
        }

        fn close(&mut self) -> api::Result<()> {
            Ok(())
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct MyFlatMapFunction {}

    impl MyFlatMapFunction {
        pub fn new() -> Self {
            MyFlatMapFunction {}
        }
    }

    impl FlatMapFunction for MyFlatMapFunction {
        fn open(&mut self, _context: &Context) -> api::Result<()> {
            Ok(())
        }

        fn flat_map(&mut self, record: Record) -> Box<dyn Iterator<Item = Record>> {
            Box::new(vec![record].into_iter())
        }

        fn close(&mut self) -> api::Result<()> {
            Ok(())
        }
    }

    impl Function for MyFlatMapFunction {
        fn get_name(&self) -> &str {
            "MyFlatMapFunction"
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct MyTimestampAssigner {}

    impl MyTimestampAssigner {
        pub fn new() -> Self {
            MyTimestampAssigner {}
        }
    }

    impl TimestampAssigner for MyTimestampAssigner {
        fn extract_timestamp(
            &mut self,
            _row: &mut Record,
            _previous_element_timestamp: u64,
        ) -> u64 {
            0
        }
    }

    impl Function for MyTimestampAssigner {
        fn get_name(&self) -> &str {
            "MyTimestampAssigner"
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct MyKeySelectorFunction {}

    impl MyKeySelectorFunction {
        pub fn new() -> Self {
            MyKeySelectorFunction {}
        }
    }

    impl KeySelectorFunction for MyKeySelectorFunction {
        fn open(&mut self, _context: &Context) -> api::Result<()> {
            Ok(())
        }

        fn get_key(&self, _record: &mut Record) -> Record {
            let record_rt = Record::new();
            record_rt
        }

        fn close(&mut self) -> api::Result<()> {
            Ok(())
        }
    }

    impl Function for MyKeySelectorFunction {
        fn get_name(&self) -> &str {
            "MyKeySelectorFunction"
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct MyReduceFunction {}

    impl MyReduceFunction {
        pub fn new() -> Self {
            MyReduceFunction {}
        }
    }

    impl ReduceFunction for MyReduceFunction {
        fn open(&mut self, _context: &Context) -> api::Result<()> {
            Ok(())
        }

        fn reduce(&self, _state_value: Option<&mut Record>, record: &mut Record) -> Record {
            record.clone()
        }

        fn close(&mut self) -> api::Result<()> {
            Ok(())
        }
    }

    impl Function for MyReduceFunction {
        fn get_name(&self) -> &str {
            "MyReduceFunction"
        }
    }

    #[derive(Debug)]
    pub struct MyOutputFormat {
        properties: Properties,
    }

    impl MyOutputFormat {
        pub fn new(properties: Properties) -> Self {
            MyOutputFormat { properties }
        }
    }

    impl OutputFormat for MyOutputFormat {
        fn open(&mut self, _context: &Context) -> api::Result<()> {
            Ok(())
        }

        fn write_record(&mut self, _record: Record) {}

        fn close(&mut self) -> api::Result<()> {
            Ok(())
        }
    }

    impl Function for MyOutputFormat {
        fn get_name(&self) -> &str {
            "MyOutputFormat"
        }
    }

    pub struct MyCoProcessFunction {}

    impl CoProcessFunction for MyCoProcessFunction {
        fn open(&mut self, _context: &Context) -> api::Result<()> {
            Ok(())
        }

        fn process_left(&self, record: Record) -> Box<dyn Iterator<Item = Record>> {
            Box::new(vec![record].into_iter())
        }

        fn process_right(
            &self,
            _stream_seq: usize,
            _record: Record,
        ) -> Box<dyn Iterator<Item = Record>> {
            Box::new(vec![].into_iter())
        }

        fn close(&mut self) -> api::Result<()> {
            Ok(())
        }
    }

    impl Function for MyCoProcessFunction {
        fn get_name(&self) -> &str {
            "MyCoProcessFunction"
        }
    }
}
