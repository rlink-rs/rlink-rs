use std::cell::RefCell;
use std::rc::Rc;

use crate::core::data_stream::{DataStream, StreamBuilder};
use crate::core::function::InputFormat;
use crate::core::operator::StreamOperator;
use crate::core::properties::Properties;
use crate::core::runtime::{ClusterDescriptor, OperatorId};
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

    /// Coordinator startup event. Called before Worker's resource allocation and startup
    fn pre_worker_startup(&self, _cluster_descriptor: &ClusterDescriptor) {}
}

#[derive(Debug)]
pub struct StreamExecutionEnvironment {
    pub(crate) stream_manager: Rc<StreamManager>,
}

impl StreamExecutionEnvironment {
    pub(crate) fn new() -> Self {
        StreamExecutionEnvironment {
            stream_manager: Rc::new(StreamManager::new()),
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

pub fn execute<S>(stream_app: S)
where
    S: StreamApp + 'static,
{
    let stream_env = StreamExecutionEnvironment::new();
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
    pub fn new() -> Self {
        StreamManager {
            stream_graph: RefCell::new(RawStreamGraph::new()),
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
