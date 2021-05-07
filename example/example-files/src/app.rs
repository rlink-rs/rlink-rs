use std::time::Duration;

use rlink::core::backend::{CheckpointBackend, KeyedStateBackend};
use rlink::core::data_stream::TDataStream;
use rlink::core::env::{StreamApp, StreamExecutionEnvironment};
use rlink::core::properties::{Properties, SystemProperties};
use rlink_example_utils::buffer_gen::model::FIELD_TYPE;
use rlink_example_utils::unbounded_input_format::RandInputFormat;

use crate::hdfs_sink::create_hdfs_sink;

#[derive(Clone, Debug)]
pub struct FilesStreamApp {}

impl StreamApp for FilesStreamApp {
    fn prepare_properties(&self, properties: &mut Properties) {
        properties.set_keyed_state_backend(KeyedStateBackend::Memory);
        properties.set_checkpoint_internal(Duration::from_secs(15));
        properties.set_checkpoint(CheckpointBackend::Memory);
    }

    fn build_stream(&self, _properties: &Properties, env: &mut StreamExecutionEnvironment) {
        env.register_source(RandInputFormat::new(), 3)
            .add_sink(create_hdfs_sink(&FIELD_TYPE));
    }
}
