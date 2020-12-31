use crate::api::data_stream_v2::{DataStream, StreamBuilder};
use crate::api::function::InputFormat;
use crate::api::operator::StreamOperatorWrap;
use crate::api::properties::Properties;
use std::cell::RefCell;
use std::rc::Rc;

/// define a stream job
pub trait StreamJob: Send + Sync + Clone {
    /// prepare job properties
    /// only invoke once on the `Coordinator`
    /// All initialization operations should be handled in this method
    fn prepare_properties(&self, properties: &mut Properties);

    /// build job stream
    /// will invoke many times on the `Coordinator` and `Worker`
    /// ensure the method is stateless
    fn build_stream(&self, properties: &Properties, env: &StreamExecutionEnvironment);
}

#[derive(Debug)]
pub struct StreamExecutionEnvironment {
    pub(crate) job_name: String,

    pub(crate) stream_builder_manager: Rc<PipelineStreamManager>,
}

impl StreamExecutionEnvironment {
    fn new(job_name: String) -> Self {
        StreamExecutionEnvironment {
            job_name,
            stream_builder_manager: Rc::new(PipelineStreamManager::new()),
        }
    }

    pub fn register_source<I>(&mut self, input_format: I, parallelism: u32) -> DataStream
    where
        I: InputFormat + 'static,
    {
        let stream_builder = StreamBuilder::with_source(
            self.stream_builder_manager.clone(),
            Box::new(input_format),
            parallelism,
        );
        DataStream::new(stream_builder)
    }

    pub(crate) fn build_stream<S>(&self, stream_job: S, job_properties: &Properties)
    where
        S: StreamJob + 'static,
    {
        stream_job.build_stream(job_properties, self);
    }
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
pub(crate) struct PipelineStreamManager {
    pub(crate) id_gen: RefCell<IdGen>,
    pub(crate) pipeline_stream_operators: RefCell<Vec<Vec<StreamOperatorWrap>>>,
}

impl PipelineStreamManager {
    pub fn new() -> Self {
        PipelineStreamManager {
            id_gen: RefCell::new(IdGen::new()),
            pipeline_stream_operators: RefCell::new(Vec::new()),
        }
    }

    pub fn get(&self) -> u32 {
        self.id_gen.borrow().get()
    }

    pub fn next(&self) -> u32 {
        self.id_gen.borrow_mut().next()
    }

    pub fn add_pipeline(&self, stream_operators: Vec<StreamOperatorWrap>) {
        self.pipeline_stream_operators
            .borrow_mut()
            .push(stream_operators);
    }
}
