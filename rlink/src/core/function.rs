use std::fmt::Debug;

use crate::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::{Element, Record};
use crate::core::properties::Properties;
use crate::core::runtime::{CheckpointId, OperatorId, TaskId};
use crate::dag::execution_graph::{ExecutionEdge, ExecutionNode};

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

    pub(crate) children: Vec<(ExecutionNode, ExecutionEdge)>,
    pub(crate) parents: Vec<(ExecutionNode, ExecutionEdge)>,
}

impl Context {
    pub fn checkpoint_context(&self) -> FunctionSnapshotContext {
        FunctionSnapshotContext::new(
            self.operator_id,
            self.task_id,
            self.checkpoint_id,
            self.completed_checkpoint_id,
        )
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

/// The base interface for data sources that produces records.
///
pub trait InputFormat
where
    Self: InputSplitSource + NamedFunction + CheckpointFunction,
{
    // fn configure(&mut self, properties: HashMap<String, String>);
    fn open(&mut self, input_split: InputSplit, context: &Context) -> crate::core::Result<()>;
    fn record_iter(&mut self) -> Box<dyn Iterator<Item = Record> + Send>;
    fn element_iter(&mut self) -> Box<dyn Iterator<Item = Element> + Send> {
        Box::new(ElementIterator::new(self.record_iter()))
    }
    fn close(&mut self) -> crate::core::Result<()>;
    fn daemon(&self) -> bool {
        false
    }
}

pub trait OutputFormat
where
    Self: NamedFunction + CheckpointFunction,
{
    /// Opens a parallel instance of the output format to store the result of its parallel instance.
    ///
    /// When this method is called, the output format it guaranteed to be configured.
    ///
    /// `taskNumber` The number of the parallel instance.
    /// `numTasks` The number of parallel tasks.
    fn open(&mut self, context: &Context) -> crate::core::Result<()>;

    fn write_record(&mut self, record: Record);

    fn write_element(&mut self, element: Element) {
        self.write_record(element.into_record())
    }

    fn close(&mut self) -> crate::core::Result<()>;

    // todo unsupported. `TwoPhaseCommitSinkFunction`
    // fn begin_transaction(&mut self) {}
    // fn prepare_commit(&mut self) {}
    // fn commit(&mut self) {}
    // fn abort(&mut self) {}
}

pub trait FlatMapFunction
where
    Self: NamedFunction + CheckpointFunction,
{
    fn open(&mut self, context: &Context) -> crate::core::Result<()>;
    fn flat_map(&mut self, record: Record) -> Box<dyn Iterator<Item = Record>>;
    fn flat_map_element(&mut self, element: Element) -> Box<dyn Iterator<Item = Element>> {
        let iterator = self.flat_map(element.into_record());
        Box::new(ElementIterator::new(iterator))
    }
    fn close(&mut self) -> crate::core::Result<()>;
}

pub trait FilterFunction
where
    Self: NamedFunction + CheckpointFunction,
{
    fn open(&mut self, context: &Context) -> crate::core::Result<()>;
    fn filter(&self, record: &mut Record) -> bool;
    fn close(&mut self) -> crate::core::Result<()>;
}

pub trait KeySelectorFunction
where
    Self: NamedFunction + CheckpointFunction,
{
    fn open(&mut self, context: &Context) -> crate::core::Result<()>;
    fn get_key(&self, record: &mut Record) -> Record;
    fn close(&mut self) -> crate::core::Result<()>;
}

pub trait ReduceFunction
where
    Self: NamedFunction,
{
    fn open(&mut self, context: &Context) -> crate::core::Result<()>;
    ///
    fn reduce(&self, value: Option<&mut Record>, record: &mut Record) -> Record;
    fn close(&mut self) -> crate::core::Result<()>;
}

pub(crate) trait BaseReduceFunction
where
    Self: NamedFunction + CheckpointFunction,
{
    fn open(&mut self, context: &Context) -> crate::core::Result<()>;
    ///
    fn reduce(&mut self, key: Record, record: Record);
    fn drop_state(&mut self, watermark_timestamp: u64) -> Vec<Record>;
    fn close(&mut self) -> crate::core::Result<()>;
}

pub trait CoProcessFunction
where
    Self: NamedFunction + CheckpointFunction,
{
    fn open(&mut self, context: &Context) -> crate::core::Result<()>;
    /// This method is called for each element in the first of the connected streams.
    ///
    /// `stream_seq` is the `DataStream` index
    fn process_left(&mut self, record: Record) -> Box<dyn Iterator<Item = Record>>;
    fn process_right(
        &mut self,
        stream_seq: usize,
        record: Record,
    ) -> Box<dyn Iterator<Item = Record>>;
    fn close(&mut self) -> crate::core::Result<()>;
}

pub(crate) struct ElementIterator<T>
where
    T: Iterator<Item = Record>,
{
    iterator: T,
}

impl<T> ElementIterator<T>
where
    T: Iterator<Item = Record>,
{
    pub fn new(iterator: T) -> Self {
        ElementIterator { iterator }
    }
}

impl<T> Iterator for ElementIterator<T>
where
    T: Iterator<Item = Record>,
{
    type Item = Element;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iterator.next() {
            Some(record) => Some(Element::Record(record)),
            None => None,
        }
    }
}
