use std::cell::RefCell;
use std::rc::Rc;

use crate::api::data_stream::{DataStream, StreamBuilder};
use crate::api::function::InputFormat;
use crate::api::operator::StreamOperatorWrap;
use crate::api::properties::Properties;
use crate::dag::stream_graph::OperatorId;
use crate::dag::RawStreamGraph;
use crate::runtime;

/// define a stream job
pub trait StreamJob: Send + Sync + Clone {
    /// prepare job properties
    /// only invoke once on the `Coordinator`
    /// All initialization operations should be handled in this method
    fn prepare_properties(&self, properties: &mut Properties);

    /// build job stream
    /// will invoke many times on the `Coordinator` and `Worker`
    /// ensure the method is stateless
    fn build_stream(&self, properties: &Properties, env: &mut StreamExecutionEnvironment);
}

#[derive(Debug)]
pub struct StreamExecutionEnvironment {
    pub(crate) application_name: String,

    pub(crate) stream_manager: Rc<StreamManager>,
}

impl StreamExecutionEnvironment {
    pub(crate) fn new(application_name: String) -> Self {
        StreamExecutionEnvironment {
            application_name: application_name.clone(),
            stream_manager: Rc::new(StreamManager::new(application_name)),
        }
    }

    pub fn register_source<I>(&mut self, input_format: I, parallelism: u32) -> DataStream
    where
        I: InputFormat + 'static,
    {
        let stream_builder = StreamBuilder::with_source(
            self.stream_manager.clone(),
            Box::new(input_format),
            parallelism,
        );
        DataStream::new(stream_builder)
    }

    // pub(crate) fn build_stream<S>(&self, stream_job: S, job_properties: &Properties)
    // where
    //     S: StreamJob + 'static,
    // {
    //     stream_job.build_stream(job_properties, self);
    // }
}

pub fn execute<S>(job_name: &str, stream_job: S)
where
    S: StreamJob + 'static,
{
    let stream_env = StreamExecutionEnvironment::new(job_name.to_string());
    runtime::run(stream_env, stream_job);
}

pub(crate) const ROOT_ID: u32 = 100;

#[derive(Debug, Clone)]
pub struct IdGen {
    id: u32,
}

impl IdGen {
    pub fn new() -> Self {
        IdGen { id: ROOT_ID }
    }

    pub fn get(&self) -> u32 {
        self.id
    }

    pub fn next(&mut self) -> u32 {
        self.id += 1;
        self.id
    }
}

#[derive(Debug)]
pub(crate) struct StreamManager {
    pub(crate) stream_graph: RefCell<RawStreamGraph>,
}

impl StreamManager {
    pub fn new(application_name: String) -> Self {
        StreamManager {
            stream_graph: RefCell::new(RawStreamGraph::new(application_name)),
        }
    }

    pub fn add_operator(
        &self,
        operator: StreamOperatorWrap,
        parent_operator_ids: Vec<OperatorId>,
    ) -> OperatorId {
        self.stream_graph
            .borrow_mut()
            .add_operator(operator, parent_operator_ids)
            .expect("add operator error")
    }
}
