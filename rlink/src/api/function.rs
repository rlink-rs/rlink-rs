use crate::api::checkpoint::{CheckpointHandle, FunctionSnapshotContext};
use crate::api::element::Record;
use crate::api::properties::Properties;
use std::fmt::Debug;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Context {
    pub job_id: String,
    pub job_properties: Properties,
    pub task_id: String,
    /// task sequence number. each chain，task sequence number start at 0.
    pub task_number: u16,
    /// total number tasks in the chain.
    pub num_tasks: u16,
    pub chain_id: u32,
    pub dependency_chain_id: u32,
    pub checkpoint_id: u64,
    pub checkpoint_handle: Option<CheckpointHandle>,
}

impl Context {
    pub fn get_checkpoint_context(&self) -> FunctionSnapshotContext {
        FunctionSnapshotContext::new(self.chain_id, self.task_number, self.checkpoint_id)
    }
}

/// Base class of all operators in the Rust API.
pub trait Function {
    fn get_name(&self) -> &str;
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
    fn filter(&self, t: &mut Record) -> bool;
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
