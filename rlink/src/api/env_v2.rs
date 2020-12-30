use crate::api::data_stream_v2::{DataStream, StreamBuilder};
use crate::api::function::InputFormat;
use crate::api::properties::Properties;
use crate::runtime;
use std::ops::Add;
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
    pub(crate) id_gen: IdGen,

    pub(crate) pipeline_stream_builders: Vec<StreamBuilder>,
}

impl StreamExecutionEnvironment {
    fn new(job_name: String) -> Self {
        StreamExecutionEnvironment {
            job_name,
            id_gen: IdGen::new(),
            pipeline_stream_builders: Vec::new(),
        }
    }

    pub fn register_source<I>(&mut self, input_format: I, parallelism: u32) -> DataStream
    where
        I: InputFormat + 'static,
    {
        let stream_builder =
            StreamBuilder::with_source(self.id_gen.clone(), Box::new(input_format), parallelism);
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
    id: Rc<u32>,
}

impl IdGen {
    pub fn new() -> Self {
        IdGen {
            id: Rc::new(ROOT_ID),
        }
    }

    pub fn get(&self) -> u32 {
        *self.id
    }

    pub fn next(&self) -> u32 {
        self.id.add(1)
    }
}
