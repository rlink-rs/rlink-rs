use std::fmt::Debug;

use crate::api::checkpoint::{CheckpointHandle, CheckpointedFunction, FunctionSnapshotContext};
use crate::api::element::{Element, Record};
use crate::api::properties::Properties;
use crate::dag::TaskId;

/// Base class of all operators in the Rust API.
pub trait Function {
    fn get_name(&self) -> &str;
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Context {
    pub application_id: String,
    pub application_properties: Properties,
    pub task_id: TaskId,

    pub checkpoint_id: u64,
    pub checkpoint_handle: Option<CheckpointHandle>,
}

impl Context {
    pub fn get_checkpoint_context(&self) -> FunctionSnapshotContext {
        FunctionSnapshotContext::new(
            self.task_id.job_id,
            self.task_id.task_number,
            self.checkpoint_id,
        )
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct InputSplit {
    split_number: u32,
    properties: Properties,
}

impl InputSplit {
    pub fn new(split_number: u32, properties: Properties) -> Self {
        InputSplit {
            split_number,
            properties,
        }
    }

    pub fn get_split_number(&self) -> u32 {
        self.split_number
    }

    pub fn get_properties(&self) -> &Properties {
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

    pub fn get_next_input_split(&mut self, _host: String, _task_id: usize) -> Option<InputSplit> {
        self.input_splits.pop()
    }
}

/// InputSplitSources create InputSplit that define portions of data to be produced by `InputFormat`
///
pub trait InputSplitSource {
    /// Create InputSplits by system parallelism[`min_num_splits`]
    ///
    /// Returns a InputSplit vec
    fn create_input_splits(&self, min_num_splits: u32) -> Vec<InputSplit>;

    /// Create InputSplitAssigner by InputSplits['input_splits']
    ///
    /// Returns a InputSplitAssigner
    fn get_input_split_assigner(&self, input_splits: Vec<InputSplit>) -> InputSplitAssigner;
}

/// The base interface for data sources that produces records.
///
pub trait InputFormat
where
    Self: InputSplitSource + Function,
{
    // fn configure(&mut self, properties: HashMap<String, String>);
    fn open(&mut self, input_split: InputSplit, context: &Context);
    fn reached_end(&self) -> bool;
    fn next_record(&mut self) -> Option<Record>;
    fn next_element(&mut self) -> Option<Element> {
        self.next_record().map(|record| Element::Record(record))
    }
    fn close(&mut self);

    fn get_checkpoint(&mut self) -> Option<Box<&mut dyn CheckpointedFunction>> {
        None
    }
}

pub trait OutputFormat
where
    Self: Function,
{
    /// Opens a parallel instance of the output format to store the result of its parallel instance.
    ///
    /// When this method is called, the output format it guaranteed to be configured.
    ///
    /// `taskNumber` The number of the parallel instance.
    /// `numTasks` The number of parallel tasks.
    fn open(&mut self, context: &Context);

    fn write_record(&mut self, record: Record);

    fn write_element(&mut self, element: Element) {
        self.write_record(element.into_record())
    }

    fn close(&mut self);

    // todo unsupported. `TwoPhaseCommitSinkFunction`
    fn begin_transaction(&mut self) {}
    fn prepare_commit(&mut self) {}
    fn commit(&mut self) {}
    fn abort(&mut self) {}
}

pub trait MapFunction
where
    Self: Function,
{
    fn open(&mut self, context: &Context);
    fn map(&mut self, record: &mut Record) -> Vec<Record>;
    fn close(&mut self);
}

pub trait FilterFunction
where
    Self: Function,
{
    fn open(&mut self, context: &Context);
    fn filter(&self, record: &mut Record) -> bool;
    fn close(&mut self);
}

pub trait KeySelectorFunction
where
    Self: Function,
{
    fn open(&mut self, context: &Context);
    fn get_key(&self, record: &mut Record) -> Record;
    fn close(&mut self);
}

pub trait ReduceFunction
where
    Self: Function,
{
    fn open(&mut self, context: &Context);
    ///
    fn reduce(&self, value: Option<&mut Record>, record: &mut Record) -> Record;
    fn close(&mut self);
}

pub trait CoProcessFunction
where
    Self: Function,
{
    fn open(&mut self, context: &Context);
    /// This method is called for each element in the first of the connected streams.
    ///
    /// `stream_seq` is the `DataStream` index
    fn process(&self, stream_seq: usize, record: Record) -> Record;
    fn close(&mut self);
}
