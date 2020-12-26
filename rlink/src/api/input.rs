use crate::api::checkpoint::CheckpointedFunction;
use crate::api::element::{Element, Record};
use crate::api::function::{Context, Function};
use crate::api::split::{InputSplit, InputSplitAssigner};

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
