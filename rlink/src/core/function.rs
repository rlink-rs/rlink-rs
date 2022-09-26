use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;

use crate::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::{Element, FnSchema, Record};
use crate::core::properties::Properties;
use crate::core::runtime::{CheckpointId, OperatorId, TaskId};
use crate::dag::execution_graph::{ExecutionEdge, ExecutionNode};
use crate::runtime::worker::WorkerTaskContext;

/// Base class of all operators in the Rust API.
pub trait NamedFunction {
    fn name(&self) -> &str;
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Context {
    pub application_id: String,
    pub application_properties: Properties,
    pub operator_id: OperatorId,
    pub task_id: TaskId,

    pub checkpoint_id: CheckpointId,
    pub completed_checkpoint_id: Option<CheckpointId>,
    pub checkpoint_handle: Option<CheckpointHandle>,

    pub input_schema: FnSchema,
    pub output_schema: FnSchema,

    pub(crate) children: Vec<(ExecutionNode, ExecutionEdge)>,
    pub(crate) parents: Vec<(ExecutionNode, ExecutionEdge)>,

    // #[serde(skip)]
    // pub(crate) cluster_descriptor: Arc<ClusterDescriptor>,
    // #[serde(skip)]
    // pub(crate) checkpoint_publish: Arc<CheckpointPublish>,
    #[serde(skip)]
    pub(crate) task_context: Option<Arc<WorkerTaskContext>>,
}

impl Context {
    pub fn checkpoint_context(&self) -> FunctionSnapshotContext {
        FunctionSnapshotContext::new(
            self.operator_id,
            self.task_id,
            self.checkpoint_id,
            self.completed_checkpoint_id,
            self.task_context(),
        )
    }

    pub(crate) fn task_context(&self) -> Arc<WorkerTaskContext> {
        self.task_context.as_ref().unwrap().clone()
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct InputSplit {
    split_number: u16,
    properties: Properties,
}

impl InputSplit {
    pub fn new(split_number: u16, properties: Properties) -> Self {
        InputSplit {
            split_number,
            properties,
        }
    }

    pub fn split_number(&self) -> u16 {
        self.split_number
    }

    pub fn properties(&self) -> &Properties {
        &self.properties
    }
}

impl Default for InputSplit {
    fn default() -> Self {
        InputSplit::new(0, Properties::new())
    }
}

pub struct InputSplitAssigner {
    input_splits: Vec<InputSplit>,
}

impl InputSplitAssigner {
    pub fn new(input_splits: Vec<InputSplit>) -> Self {
        InputSplitAssigner { input_splits }
    }

    pub fn next_input_split(&mut self, _host: String, _task_id: usize) -> Option<InputSplit> {
        self.input_splits.pop()
    }
}

/// InputSplitSources create InputSplit that define portions of data to be produced by `InputFormat`
///
#[async_trait]
pub trait InputSplitSource {
    /// Create InputSplits by system parallelism[`min_num_splits`]
    ///
    /// Returns a InputSplit vec
    fn create_input_splits(&self, min_num_splits: u16) -> crate::core::Result<Vec<InputSplit>> {
        let mut input_splits = Vec::with_capacity(min_num_splits as usize);
        for task_number in 0..min_num_splits {
            input_splits.push(InputSplit::new(task_number, Properties::new()));
        }
        Ok(input_splits)
    }

    /// Create InputSplitAssigner by InputSplits['input_splits']
    ///
    /// Returns a InputSplitAssigner
    fn input_split_assigner(&self, input_splits: Vec<InputSplit>) -> InputSplitAssigner {
        InputSplitAssigner::new(input_splits)
    }
}

/// Trait for types that stream [rlink::core::Element]
pub trait ElementStream: Stream<Item = Element> {}

/// Trait for a stream of record batches.
pub type SendableElementStream = Pin<Box<dyn ElementStream + Send>>;

/// The base interface for data sources that produces records.
///
#[async_trait]
pub trait InputFormat
where
    Self: InputSplitSource + NamedFunction + CheckpointFunction + Send + Sync,
{
    /// Initialization of `InputFormat`, Each task will be called once when it starts.
    async fn open(&mut self, input_split: InputSplit, context: &Context)
        -> crate::core::Result<()>;
    /// return an `Stream` of `Element`, if the `next` of `Stream` is `None`,
    /// the task of `InputFormat` will be `Terminated`.
    /// the function is called by runtime
    async fn element_stream(&mut self) -> SendableElementStream;

    async fn close(&mut self) -> crate::core::Result<()>;

    /// mark the `InputFormat` is running in daemon mode,
    /// if `true`, this `InputFormat` is automatically terminated when any task instance ends
    /// todo check daemon by whether there are some parent jobs
    fn daemon(&self) -> bool {
        false
    }

    fn schema(&self, input_schema: FnSchema) -> FnSchema;

    fn parallelism(&self) -> u16;
}

#[async_trait]
pub trait OutputFormat
where
    Self: NamedFunction + CheckpointFunction + Send + Sync,
{
    /// Opens a parallel instance of the output format to store the result of its parallel instance.
    ///
    /// When this method is called, the output format it guaranteed to be configured.
    ///
    /// `taskNumber` The number of the parallel instance.
    /// `numTasks` The number of parallel tasks.
    async fn open(&mut self, context: &Context) -> crate::core::Result<()>;

    async fn write_element(&mut self, element: Element);

    async fn close(&mut self) -> crate::core::Result<()>;

    // todo unsupported. `TwoPhaseCommitSinkFunction`
    // fn begin_transaction(&mut self) {}
    // fn prepare_commit(&mut self) {}
    // fn commit(&mut self) {}
    // fn abort(&mut self) {}

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::Empty
    }
}

#[async_trait]
pub trait FlatMapFunction
where
    Self: NamedFunction + CheckpointFunction + Send + Sync,
{
    async fn open(&mut self, context: &Context) -> crate::core::Result<()>;
    async fn flat_map_element(&mut self, element: Element) -> SendableElementStream;
    async fn close(&mut self) -> crate::core::Result<()>;

    fn schema(&self, input_schema: FnSchema) -> FnSchema;
}

#[async_trait]
pub trait FilterFunction
where
    Self: NamedFunction + CheckpointFunction + Send + Sync,
{
    async fn open(&mut self, context: &Context) -> crate::core::Result<()>;

    async fn filter(&self, record: &mut Record) -> bool;

    async fn close(&mut self) -> crate::core::Result<()>;
}

#[async_trait]
pub trait KeySelectorFunction
where
    Self: NamedFunction + CheckpointFunction + Send + Sync,
{
    async fn open(&mut self, context: &Context) -> crate::core::Result<()>;

    async fn get_key(&self, record: &mut Record) -> Record;

    async fn close(&mut self) -> crate::core::Result<()>;

    fn key_schema(&self, input_schema: FnSchema) -> FnSchema;
}

#[async_trait]
pub trait ReduceFunction
where
    Self: NamedFunction + Send + Sync,
{
    async fn open(&mut self, context: &Context) -> crate::core::Result<()>;

    fn reduce(&self, value: Option<&mut Record>, record: &mut Record) -> Record;

    async fn close(&mut self) -> crate::core::Result<()>;

    fn schema(&self, input_schema: FnSchema) -> FnSchema;

    fn parallelism(&self) -> u16;
}

#[async_trait]
pub(crate) trait BaseReduceFunction
where
    Self: NamedFunction + CheckpointFunction + Send + Sync,
{
    async fn open(&mut self, context: &Context) -> crate::core::Result<()>;

    async fn reduce(&mut self, key: Record, record: Record);

    async fn drop_state(&mut self, watermark_timestamp: u64) -> Vec<Record>;

    async fn close(&mut self) -> crate::core::Result<()>;

    fn value_schema(&self, key_schema: FnSchema) -> FnSchema;
}

#[async_trait]
pub trait CoProcessFunction
where
    Self: NamedFunction + CheckpointFunction + Send + Sync,
{
    async fn open(&mut self, context: &Context) -> crate::core::Result<()>;

    /// This method is called for each element in the first of the connected streams.
    ///
    /// `stream_seq` is the `DataStream` index
    async fn process_left(&mut self, record: Record) -> SendableElementStream;

    async fn process_right(&mut self, stream_seq: usize, record: Record) -> SendableElementStream;

    async fn close(&mut self) -> crate::core::Result<()>;

    fn schema(&self, input_schema: FnSchema) -> FnSchema;
}
