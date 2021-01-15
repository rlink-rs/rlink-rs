use std::fmt::Debug;

use crate::api::function::{
    CoProcessFunction, FilterFunction, FlatMapFunction, Function, InputFormat, KeySelectorFunction,
    OutputFormat, ReduceFunction,
};
use crate::api::watermark::WatermarkAssigner;
use crate::api::window::WindowAssigner;

pub const DEFAULT_PARALLELISM: u32 = 0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionCreator {
    System = 0,
    User = 1,
}

pub trait TStreamOperator: Debug {
    fn get_operator_name(&self) -> &str;
    fn get_parallelism(&self) -> u32;
    fn get_fn_creator(&self) -> FunctionCreator;
}

pub struct StreamOperator<T>
where
    T: ?Sized + Function,
{
    parallelism: u32,
    fn_creator: FunctionCreator,
    pub(crate) operator_fn: Box<T>,
}

impl<T> StreamOperator<T>
where
    T: ?Sized + Function,
{
    pub fn new(parallelism: u32, fn_creator: FunctionCreator, operator_fn: Box<T>) -> Self {
        StreamOperator {
            parallelism,
            fn_creator,
            operator_fn,
        }
    }
}

impl<T> TStreamOperator for StreamOperator<T>
where
    T: ?Sized + Function,
{
    fn get_operator_name(&self) -> &str {
        self.operator_fn.get_name()
    }

    fn get_parallelism(&self) -> u32 {
        self.parallelism
    }

    fn get_fn_creator(&self) -> FunctionCreator {
        self.fn_creator.clone()
    }
}

impl<T> Debug for StreamOperator<T>
where
    T: ?Sized + Function,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamOperator")
            .field("parallelism", &self.parallelism)
            .field("fn_creator", &self.fn_creator)
            .field("operator_fn", &self.operator_fn.get_name())
            .finish()
    }
}

#[derive(Debug)]
pub enum StreamOperatorWrap {
    StreamSource(StreamOperator<dyn InputFormat>),
    StreamMap(StreamOperator<dyn FlatMapFunction>),
    StreamFilter(StreamOperator<dyn FilterFunction>),
    StreamCoProcess(StreamOperator<dyn CoProcessFunction>),
    StreamKeyBy(StreamOperator<dyn KeySelectorFunction>),
    StreamReduce(StreamOperator<dyn ReduceFunction>),
    StreamWatermarkAssigner(StreamOperator<dyn WatermarkAssigner>),
    StreamWindowAssigner(StreamOperator<dyn WindowAssigner>),
    StreamSink(StreamOperator<dyn OutputFormat>),
}

impl StreamOperatorWrap {
    pub fn new_source(
        parallelism: u32,
        fn_creator: FunctionCreator,
        source_fn: Box<dyn InputFormat>,
    ) -> Self {
        let operator = StreamOperator::new(parallelism, fn_creator, source_fn);
        StreamOperatorWrap::StreamSource(operator)
    }

    pub fn new_map(map_fn: Box<dyn FlatMapFunction>) -> Self {
        let operator = StreamOperator::new(DEFAULT_PARALLELISM, FunctionCreator::User, map_fn);
        StreamOperatorWrap::StreamMap(operator)
    }

    pub fn new_filter(filter_fn: Box<dyn FilterFunction>) -> Self {
        let operator = StreamOperator::new(DEFAULT_PARALLELISM, FunctionCreator::User, filter_fn);
        StreamOperatorWrap::StreamFilter(operator)
    }

    pub fn new_co_process(co_process_fn: Box<dyn CoProcessFunction>) -> Self {
        let operator =
            StreamOperator::new(DEFAULT_PARALLELISM, FunctionCreator::User, co_process_fn);
        StreamOperatorWrap::StreamCoProcess(operator)
    }

    pub fn new_key_by(key_by_fn: Box<dyn KeySelectorFunction>) -> Self {
        let operator = StreamOperator::new(DEFAULT_PARALLELISM, FunctionCreator::User, key_by_fn);
        StreamOperatorWrap::StreamKeyBy(operator)
    }

    pub fn new_reduce(parallelism: u32, reduce_fn: Box<dyn ReduceFunction>) -> Self {
        let operator = StreamOperator::new(parallelism, FunctionCreator::User, reduce_fn);
        StreamOperatorWrap::StreamReduce(operator)
    }

    pub fn new_watermark_assigner(watermark_assigner: Box<dyn WatermarkAssigner>) -> Self {
        let operator = StreamOperator::new(
            DEFAULT_PARALLELISM,
            FunctionCreator::User,
            watermark_assigner,
        );
        StreamOperatorWrap::StreamWatermarkAssigner(operator)
    }

    pub fn new_window_assigner(window_assigner: Box<dyn WindowAssigner>) -> Self {
        let operator =
            StreamOperator::new(DEFAULT_PARALLELISM, FunctionCreator::User, window_assigner);
        StreamOperatorWrap::StreamWindowAssigner(operator)
    }

    pub fn new_sink(fn_creator: FunctionCreator, sink_fn: Box<dyn OutputFormat>) -> Self {
        let operator = StreamOperator::new(DEFAULT_PARALLELISM, fn_creator, sink_fn);
        StreamOperatorWrap::StreamSink(operator)
    }

    pub fn is_source(&self) -> bool {
        if let StreamOperatorWrap::StreamSource(_stream_source) = self {
            return true;
        }
        false
    }

    pub fn is_window(&self) -> bool {
        if let StreamOperatorWrap::StreamWindowAssigner(_stream_window) = self {
            return true;
        }
        false
    }

    pub fn is_key_by(&self) -> bool {
        if let StreamOperatorWrap::StreamKeyBy(_stream_key_by) = self {
            return true;
        }
        false
    }

    pub fn is_reduce(&self) -> bool {
        if let StreamOperatorWrap::StreamReduce(_stream_reduce) = self {
            return true;
        }
        false
    }

    pub fn is_sink(&self) -> bool {
        if let StreamOperatorWrap::StreamSink(_stream_sink) = self {
            return true;
        }
        false
    }

    pub fn is_map(&self) -> bool {
        if let StreamOperatorWrap::StreamMap(_stream_map) = self {
            return true;
        }
        false
    }

    pub fn is_filter(&self) -> bool {
        if let StreamOperatorWrap::StreamFilter(_stream_filter) = self {
            return true;
        }
        false
    }

    pub fn is_connect(&self) -> bool {
        if let StreamOperatorWrap::StreamCoProcess(_stream_source) = self {
            return true;
        }
        false
    }
}

impl TStreamOperator for StreamOperatorWrap {
    fn get_operator_name(&self) -> &str {
        match self {
            StreamOperatorWrap::StreamSource(op) => op.get_operator_name(),
            StreamOperatorWrap::StreamMap(op) => op.get_operator_name(),
            StreamOperatorWrap::StreamFilter(op) => op.get_operator_name(),
            StreamOperatorWrap::StreamCoProcess(op) => op.get_operator_name(),
            StreamOperatorWrap::StreamKeyBy(op) => op.get_operator_name(),
            StreamOperatorWrap::StreamReduce(op) => op.get_operator_name(),
            StreamOperatorWrap::StreamWatermarkAssigner(op) => op.get_operator_name(),
            StreamOperatorWrap::StreamWindowAssigner(op) => op.get_operator_name(),
            StreamOperatorWrap::StreamSink(op) => op.get_operator_name(),
        }
    }

    fn get_parallelism(&self) -> u32 {
        match self {
            StreamOperatorWrap::StreamSource(op) => op.get_parallelism(),
            StreamOperatorWrap::StreamMap(op) => op.get_parallelism(),
            StreamOperatorWrap::StreamFilter(op) => op.get_parallelism(),
            StreamOperatorWrap::StreamCoProcess(op) => op.get_parallelism(),
            StreamOperatorWrap::StreamKeyBy(op) => op.get_parallelism(),
            StreamOperatorWrap::StreamReduce(op) => op.get_parallelism(),
            StreamOperatorWrap::StreamWatermarkAssigner(op) => op.get_parallelism(),
            StreamOperatorWrap::StreamWindowAssigner(op) => op.get_parallelism(),
            StreamOperatorWrap::StreamSink(op) => op.get_parallelism(),
        }
    }

    fn get_fn_creator(&self) -> FunctionCreator {
        match self {
            StreamOperatorWrap::StreamSource(op) => op.get_fn_creator(),
            StreamOperatorWrap::StreamMap(op) => op.get_fn_creator(),
            StreamOperatorWrap::StreamFilter(op) => op.get_fn_creator(),
            StreamOperatorWrap::StreamCoProcess(op) => op.get_fn_creator(),
            StreamOperatorWrap::StreamKeyBy(op) => op.get_fn_creator(),
            StreamOperatorWrap::StreamReduce(op) => op.get_fn_creator(),
            StreamOperatorWrap::StreamWatermarkAssigner(op) => op.get_fn_creator(),
            StreamOperatorWrap::StreamWindowAssigner(op) => op.get_fn_creator(),
            StreamOperatorWrap::StreamSink(op) => op.get_fn_creator(),
        }
    }
}
