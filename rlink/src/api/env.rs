use std::cell::RefCell;
use std::rc::Rc;

use crate::api::data_stream::{DataStream, StreamBuilder};
use crate::api::function::InputFormat;
use crate::api::operator::StreamOperator;
use crate::api::properties::Properties;
use crate::api::runtime::OperatorId;
use crate::dag::RawStreamGraph;
use crate::runtime;

/// define a stream application
pub trait StreamApp: Send + Sync + Clone {
    /// prepare job properties,
    /// only invoke once on the `Coordinator`,
    /// All initialization operations should be handled in this method.
    fn prepare_properties(&self, properties: &mut Properties);

    /// build application stream,
    /// will invoke multiple times on the `Coordinator` and `Worker`,
    /// ensure the method is stateless.
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

    pub fn register_source<I>(&mut self, input_format: I, parallelism: u16) -> DataStream
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
}

pub fn execute<S>(application_name: &str, stream_app: S)
where
    S: StreamApp + 'static,
{
    let stream_env = StreamExecutionEnvironment::new(application_name.to_string());
    match runtime::run(stream_env, stream_app) {
        Ok(_) => {}
        Err(e) => {
            panic!(
                "force panic when catch error in job startup process. msg: {}",
                e
            );
        }
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
        operator: StreamOperator,
        parent_operator_ids: Vec<OperatorId>,
    ) -> OperatorId {
        self.stream_graph
            .borrow_mut()
            .add_operator(operator, parent_operator_ids)
            .expect("add operator error")
    }
}
