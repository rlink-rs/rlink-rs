use std::fmt::Debug;
use std::rc::Rc;

use crate::api::env::StreamManager;
use crate::api::function::{
    CoProcessFunction, FilterFunction, FlatMapFunction, InputFormat, KeySelectorFunction,
    OutputFormat, ReduceFunction,
};
use crate::api::operator::{FunctionCreator, StreamOperatorWrap};
use crate::api::runtime::OperatorId;
use crate::api::watermark::WatermarkAssigner;
use crate::api::window::WindowAssigner;

pub trait TDataStream {
    fn flat_map<F>(self, flat_mapper: F) -> DataStream
    where
        F: FlatMapFunction + 'static;

    fn filter<F>(self, filter: F) -> DataStream
    where
        F: FilterFunction + 'static;

    fn key_by<F>(self, key_selector: F) -> KeyedStream
    where
        F: KeySelectorFunction + 'static;

    fn assign_timestamps_and_watermarks<W>(self, timestamp_and_watermark_assigner: W) -> DataStream
    where
        W: WatermarkAssigner + 'static;

    /// Re-balance: Round-robin, Hash, Broadcast
    fn connect<F>(self, data_streams: Vec<DataStream>, f: F) -> ConnectedStreams
    where
        F: CoProcessFunction + 'static;

    // fn multiplexing(self) -> MultiplexingStream;

    fn add_sink<O>(self, output_format: O)
    where
        O: OutputFormat + 'static;
}

pub trait TConnectedStreams {
    /// Unlike flink, the `key_by` that receives only one KeySelector,
    /// that is, `CoProcessFunction` method output must have the same schema or with special flag.
    fn key_by<F>(self, key_selector: F) -> KeyedStream
    where
        F: KeySelectorFunction + 'static;
}

pub trait TKeyedStream {
    fn window<W>(self, window_assigner: W) -> WindowedStream
    where
        W: WindowAssigner + 'static;
    fn add_sink<O>(self, output_format: O) -> SinkStream
    where
        O: OutputFormat + 'static;
}

pub trait TWindowedStream {
    fn reduce<F>(self, reduce: F, parallelism: u32) -> DataStream
    where
        F: ReduceFunction + 'static;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DataStream {
    pub(crate) data_stream: StreamBuilder,
}

impl DataStream {
    pub(crate) fn new(data_stream: StreamBuilder) -> Self {
        DataStream { data_stream }
    }
}

impl TDataStream for DataStream {
    fn flat_map<F>(self, flat_mapper: F) -> DataStream
    where
        F: FlatMapFunction + 'static,
    {
        self.data_stream.flat_map(flat_mapper)
    }

    fn filter<F>(self, filter: F) -> DataStream
    where
        F: FilterFunction + 'static,
    {
        self.data_stream.filter(filter)
    }

    fn key_by<F>(self, key_selector: F) -> KeyedStream
    where
        F: KeySelectorFunction + 'static,
    {
        self.data_stream.key_by(key_selector)
    }

    fn assign_timestamps_and_watermarks<W>(self, timestamp_and_watermark_assigner: W) -> DataStream
    where
        W: WatermarkAssigner + 'static,
    {
        self.data_stream
            .assign_timestamps_and_watermarks(timestamp_and_watermark_assigner)
    }

    fn connect<F>(self, data_streams: Vec<DataStream>, co_process: F) -> ConnectedStreams
    where
        F: CoProcessFunction + 'static,
    {
        self.data_stream.connect(data_streams, co_process)
    }

    fn add_sink<O>(self, output_format: O)
    where
        O: OutputFormat + 'static,
    {
        TDataStream::add_sink(self.data_stream, output_format)
    }
}

#[derive(Debug)]
pub struct ConnectedStreams {
    co_stream: StreamBuilder,
    dependency_pipeline_ids: Vec<OperatorId>,
}

impl ConnectedStreams {
    pub(crate) fn new(co_stream: StreamBuilder, dependency_pipeline_ids: Vec<OperatorId>) -> Self {
        ConnectedStreams {
            co_stream,
            dependency_pipeline_ids,
        }
    }
}

impl TConnectedStreams for ConnectedStreams {
    fn key_by<F>(self, key_selector: F) -> KeyedStream
    where
        F: KeySelectorFunction + 'static,
    {
        self.co_stream.key_by(key_selector)
    }
}

#[derive(Debug)]
pub struct KeyedStream {
    keyed_stream: StreamBuilder,
}

impl KeyedStream {
    pub(crate) fn new(keyed_stream: StreamBuilder) -> Self {
        KeyedStream { keyed_stream }
    }
}

impl TKeyedStream for KeyedStream {
    fn window<W>(self, window_assigner: W) -> WindowedStream
    where
        W: WindowAssigner + 'static,
    {
        self.keyed_stream.window(window_assigner)
    }

    fn add_sink<O>(self, output_format: O) -> SinkStream
    where
        O: OutputFormat + 'static,
    {
        TKeyedStream::add_sink(self.keyed_stream, output_format)
    }
}

#[derive(Debug)]
pub struct WindowedStream {
    windowed_stream: StreamBuilder,
}

impl WindowedStream {
    pub(crate) fn new(windowed_stream: StreamBuilder) -> Self {
        WindowedStream { windowed_stream }
    }
}

impl TWindowedStream for WindowedStream {
    fn reduce<F>(self, reduce: F, parallelism: u32) -> DataStream
    where
        F: ReduceFunction + 'static,
    {
        self.windowed_stream.reduce(reduce, parallelism)
    }
}

#[derive(Debug)]
pub struct SinkStream {
    end_stream: StreamBuilder,
}

impl SinkStream {
    pub(crate) fn new(end_stream: StreamBuilder) -> Self {
        SinkStream { end_stream }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct StreamBuilder {
    current_id: u32,

    cur_operator_id: OperatorId,
    stream_manager: Rc<StreamManager>,
}

impl StreamBuilder {
    pub fn with_source(
        stream_manager: Rc<StreamManager>,
        source_func: Box<dyn InputFormat>,
        parallelism: u32,
    ) -> Self {
        let source_operator =
            StreamOperatorWrap::new_source(parallelism, FunctionCreator::User, source_func);
        let operator_id = stream_manager.add_operator(source_operator, vec![]);

        StreamBuilder {
            current_id: 0,
            cur_operator_id: operator_id,
            stream_manager,
        }
    }

    pub fn with_connect(
        stream_manager: Rc<StreamManager>,
        co_process_func: Box<dyn CoProcessFunction>,
        parent_ids: Vec<OperatorId>,
    ) -> Self {
        let co_operator = StreamOperatorWrap::new_co_process(co_process_func);
        let operator_id = stream_manager.add_operator(co_operator, parent_ids);

        StreamBuilder {
            current_id: 0,
            cur_operator_id: operator_id,
            stream_manager,
        }
    }
}

impl TDataStream for StreamBuilder {
    fn flat_map<F>(mut self, flat_mapper: F) -> DataStream
    where
        F: FlatMapFunction + 'static,
    {
        let map_func = Box::new(flat_mapper);
        let stream_map = StreamOperatorWrap::new_map(map_func);

        self.cur_operator_id = self
            .stream_manager
            .add_operator(stream_map, vec![self.cur_operator_id]);

        DataStream::new(self)
    }

    fn filter<F>(mut self, filter: F) -> DataStream
    where
        F: FilterFunction + 'static,
    {
        let filter_func = Box::new(filter);
        let stream_filter = StreamOperatorWrap::new_filter(filter_func);

        self.cur_operator_id = self
            .stream_manager
            .add_operator(stream_filter, vec![self.cur_operator_id]);

        DataStream::new(self)
    }

    fn key_by<F>(mut self, key_selector: F) -> KeyedStream
    where
        F: KeySelectorFunction + 'static,
    {
        let key_selector_func = Box::new(key_selector);
        let stream_key_by = StreamOperatorWrap::new_key_by(key_selector_func);

        self.cur_operator_id = self
            .stream_manager
            .add_operator(stream_key_by, vec![self.cur_operator_id]);

        KeyedStream::new(self)
    }

    fn assign_timestamps_and_watermarks<W>(
        mut self,
        timestamp_and_watermark_assigner: W,
    ) -> DataStream
    where
        W: WatermarkAssigner + 'static,
    {
        let time_assigner_func = Box::new(timestamp_and_watermark_assigner);
        let stream_watermark_assigner =
            StreamOperatorWrap::new_watermark_assigner(time_assigner_func);

        self.cur_operator_id = self
            .stream_manager
            .add_operator(stream_watermark_assigner, vec![self.cur_operator_id]);

        DataStream::new(self)
    }

    fn connect<F>(self, data_streams: Vec<DataStream>, co_process: F) -> ConnectedStreams
    where
        F: CoProcessFunction + 'static,
    {
        let pipeline_stream_manager = self.stream_manager.clone();

        // Ensure `this` is placed in the last position!!!
        // index trigger `process_left` at the last location, otherwise trigger `process_right`
        let mut dependency_streams: Vec<StreamBuilder> =
            data_streams.into_iter().map(|x| x.data_stream).collect();
        dependency_streams.push(self);

        let parent_ids: Vec<OperatorId> = dependency_streams
            .iter()
            .map(|x| x.cur_operator_id)
            .collect();

        let co_stream = StreamBuilder::with_connect(
            pipeline_stream_manager,
            Box::new(co_process),
            parent_ids.clone(),
        );

        ConnectedStreams::new(co_stream, parent_ids)
    }

    fn add_sink<O>(mut self, output_format: O)
    where
        O: OutputFormat + 'static,
    {
        let sink_func = Box::new(output_format);
        let stream_sink = StreamOperatorWrap::new_sink(FunctionCreator::User, sink_func);

        self.cur_operator_id = self
            .stream_manager
            .add_operator(stream_sink, vec![self.cur_operator_id]);
    }
}

impl TKeyedStream for StreamBuilder {
    fn window<W>(mut self, window_assigner: W) -> WindowedStream
    where
        W: WindowAssigner + 'static,
    {
        let window_assigner_func = Box::new(window_assigner);
        let stream_window_assigner = StreamOperatorWrap::new_window_assigner(window_assigner_func);

        self.cur_operator_id = self
            .stream_manager
            .add_operator(stream_window_assigner, vec![self.cur_operator_id]);

        WindowedStream::new(self)
    }

    fn add_sink<O>(mut self, output_format: O) -> SinkStream
    where
        O: OutputFormat + 'static,
    {
        let sink_func = Box::new(output_format);
        let stream_sink = StreamOperatorWrap::new_sink(FunctionCreator::User, sink_func);

        self.cur_operator_id = self
            .stream_manager
            .add_operator(stream_sink, vec![self.cur_operator_id]);

        SinkStream::new(self)
    }
}

impl TWindowedStream for StreamBuilder {
    fn reduce<F>(mut self, reduce: F, parallelism: u32) -> DataStream
    where
        F: ReduceFunction + 'static,
    {
        let reduce_func = Box::new(reduce);
        let stream_reduce = StreamOperatorWrap::new_reduce(parallelism, reduce_func);

        self.cur_operator_id = self
            .stream_manager
            .add_operator(stream_reduce, vec![self.cur_operator_id]);

        DataStream::new(self)
    }
}
