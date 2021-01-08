use crate::api::data_stream::{DataStream, DataStreamSource, SinkStream};
use crate::api::function::InputFormat;
use crate::api::properties::Properties;
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
    fn build_stream(&self, properties: &Properties, env: &StreamExecutionEnvironment)
        -> SinkStream;
}

#[derive(Debug, Clone)]
pub struct StreamExecutionEnvironment {
    pub(crate) application_name: String,
}

impl StreamExecutionEnvironment {
    fn new(application_name: &str) -> Self {
        StreamExecutionEnvironment {
            application_name: application_name.to_string(),
        }
    }

    pub fn register_source<I>(&self, input_format: I, parallelism: u32) -> DataStream
    where
        I: InputFormat + 'static,
    {
        let data_stream = DataStreamSource::new(Box::new(input_format), parallelism);
        DataStream::DefaultDataStream(data_stream)
    }
}

pub fn execute<S>(application_name: &str, stream_job: S)
where
    S: StreamJob + 'static,
{
    let stream_env = StreamExecutionEnvironment::new(application_name);
    runtime::run(stream_env, stream_job);
}
