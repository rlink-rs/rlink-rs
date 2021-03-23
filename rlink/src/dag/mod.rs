//! DAG builder
//! stream_graph -> job_graph -> execution_graph

use std::borrow::BorrowMut;
use std::convert::TryFrom;
use std::fmt::Debug;

use thiserror::Error;

use crate::api::function::InputSplit;
use crate::api::operator::StreamOperator;
use crate::api::runtime::{JobId, OperatorId, TaskId};
use crate::dag::execution_graph::ExecutionGraph;
use crate::dag::job_graph::JobGraph;
use crate::dag::physic_graph::PhysicGraph;
use crate::dag::stream_graph::{StreamGraph, StreamNode};

pub(crate) mod execution_graph;
pub(crate) mod job_graph;
pub(crate) mod metadata;
pub(crate) mod physic_graph;
pub(crate) mod stream_graph;
pub(crate) mod utils;

use crate::api;
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

impl<'a> From<&'a StreamOperator> for OperatorType {
    fn from(op: &'a StreamOperator) -> Self {
        match op {
            StreamOperator::StreamSource(_) => OperatorType::Source,
            StreamOperator::StreamFlatMap(_) => OperatorType::FlatMap,
            StreamOperator::StreamFilter(_) => OperatorType::Filter,
            StreamOperator::StreamCoProcess(_) => OperatorType::CoProcess,
            StreamOperator::StreamKeyBy(_) => OperatorType::KeyBy,
            StreamOperator::StreamReduce(_) => OperatorType::Reduce,
            StreamOperator::StreamWatermarkAssigner(_) => OperatorType::WatermarkAssigner,
            StreamOperator::StreamWindowAssigner(_) => OperatorType::WindowAssigner,
            StreamOperator::StreamSink(_) => OperatorType::Sink,
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
    #[error("DAG wold cycle")]
    WouldCycle,
    #[error("source not found")]
    SourceNotFound,
    #[error("source not at staring")]
    SourceNotAtStarting,
    #[error("source not at ending")]
    SinkNotAtEnding,
    #[error("reduce and child output's parallelism is conflict")]
    ReduceOutputParallelismConflict,
    #[error("connect parent not found")]
    ConnectParentNotFound,
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
    #[error("operator not found. {0:?}")]
    OperatorNotFound(OperatorId),
    #[error("job not found. {0:?}")]
    JobNotFound(JobId),
    #[error("job parallelism not found")]
    JobParallelismNotFound,
    #[error(transparent)]
    OtherApiError(#[from] api::Error),
}

#[derive(Clone, Debug)]
pub(crate) struct DagManager {
    stream_graph: StreamGraph,
    job_graph: JobGraph,
    execution_graph: ExecutionGraph,
    physic_graph: PhysicGraph,
}

impl<'a> TryFrom<&'a RawStreamGraph> for DagManager {
    type Error = DagError;

    fn try_from(raw_stream_graph: &'a RawStreamGraph) -> Result<Self, Self::Error> {
        let stream_graph = StreamGraph::new(
            raw_stream_graph.sources.clone(),
            raw_stream_graph.dag.clone(),
        );

        let mut job_graph = JobGraph::new();
        job_graph.build(&stream_graph)?;

        let mut execution_graph = ExecutionGraph::new();
        execution_graph.build(&job_graph, raw_stream_graph.operators().borrow_mut())?;

        let mut physic_graph = PhysicGraph::new();
        physic_graph.build(&execution_graph);

        Ok(DagManager {
            stream_graph,
            job_graph,
            execution_graph,
            physic_graph,
        })
    }
}

impl DagManager {
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
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;
    use std::ops::Deref;
    use std::time::Duration;

    use crate::api;
    use crate::api::checkpoint::CheckpointFunction;
    use crate::api::data_stream::CoStream;
    use crate::api::data_stream::{TConnectedStreams, TKeyedStream};
    use crate::api::data_stream::{TDataStream, TWindowedStream};
    use crate::api::element::Record;
    use crate::api::env::StreamExecutionEnvironment;
    use crate::api::function::{
        CoProcessFunction, Context, FlatMapFunction, InputFormat, InputSplit, InputSplitSource,
        KeySelectorFunction, NamedFunction, OutputFormat, ReduceFunction,
    };
    use crate::api::properties::Properties;
    use crate::api::watermark::{BoundedOutOfOrdernessTimestampExtractor, TimestampAssigner};
    use crate::api::window::SlidingEventTimeWindows;
    use crate::dag::utils::JsonDag;
    use crate::dag::DagManager;

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

        let dag_manager =
            DagManager::try_from(env.stream_manager.stream_graph.borrow().deref()).unwrap();
        {
            let dag = &dag_manager.stream_graph().dag;
            println!("{:?}", dag);
            println!("{}", serde_json::to_string(&JsonDag::from(dag)).unwrap())
        }
        {
            let dag = &dag_manager.job_graph().dag;
            println!("{:?}", dag);
            println!("{}", serde_json::to_string(&JsonDag::from(dag)).unwrap())
        }

        {
            let dag = &dag_manager.execution_graph().dag;
            println!("{:?}", dag);
            println!("{}", serde_json::to_string(&JsonDag::from(dag)).unwrap())
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

    impl CheckpointFunction for MyInputFormat {}

    impl NamedFunction for MyInputFormat {
        fn name(&self) -> &str {
            "MyInputFormat"
        }
    }

    impl InputFormat for MyInputFormat {
        fn open(&mut self, _input_split: InputSplit, _context: &Context) -> api::Result<()> {
            Ok(())
        }

        fn record_iter(&mut self) -> Box<dyn Iterator<Item = Record> + Send> {
            unimplemented!()
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

    impl NamedFunction for MyFlatMapFunction {
        fn name(&self) -> &str {
            "MyFlatMapFunction"
        }
    }

    impl CheckpointFunction for MyFlatMapFunction {}

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

    impl NamedFunction for MyTimestampAssigner {
        fn name(&self) -> &str {
            "MyTimestampAssigner"
        }
    }

    impl CheckpointFunction for MyTimestampAssigner {}

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

    impl NamedFunction for MyKeySelectorFunction {
        fn name(&self) -> &str {
            "MyKeySelectorFunction"
        }
    }

    impl CheckpointFunction for MyKeySelectorFunction {}

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

    impl NamedFunction for MyReduceFunction {
        fn name(&self) -> &str {
            "MyReduceFunction"
        }
    }

    impl CheckpointFunction for MyReduceFunction {}

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

    impl NamedFunction for MyOutputFormat {
        fn name(&self) -> &str {
            "MyOutputFormat"
        }
    }

    impl CheckpointFunction for MyOutputFormat {}

    pub struct MyCoProcessFunction {}

    impl CoProcessFunction for MyCoProcessFunction {
        fn open(&mut self, _context: &Context) -> api::Result<()> {
            Ok(())
        }

        fn process_left(&mut self, record: Record) -> Box<dyn Iterator<Item = Record>> {
            Box::new(vec![record].into_iter())
        }

        fn process_right(
            &mut self,
            _stream_seq: usize,
            _record: Record,
        ) -> Box<dyn Iterator<Item = Record>> {
            Box::new(vec![].into_iter())
        }

        fn close(&mut self) -> api::Result<()> {
            Ok(())
        }
    }

    impl NamedFunction for MyCoProcessFunction {
        fn name(&self) -> &str {
            "MyCoProcessFunction"
        }
    }

    impl CheckpointFunction for MyCoProcessFunction {}
}
