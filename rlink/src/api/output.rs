use crate::api::element::{Element, Record};
use crate::api::function::{Context, Function};

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
