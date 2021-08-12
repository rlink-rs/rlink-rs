use std::fmt::Debug;

use crate::core::data_types::Schema;
use crate::core::element::FnSchema;
use crate::core::function::{
    BaseReduceFunction, CoProcessFunction, FilterFunction, FlatMapFunction, InputFormat,
    KeySelectorFunction, NamedFunction, OutputFormat,
};
use crate::core::watermark::WatermarkStrategy;
use crate::core::window::WindowAssigner;

pub const DEFAULT_PARALLELISM: u16 = 0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FunctionCreator {
    System = 0,
    User = 1,
}

pub trait TStreamOperator: Debug {
    fn operator_name(&self) -> &str;
    fn parallelism(&self) -> u16;
    fn schema(&self, input_schema: FnSchema) -> FnSchema;
    fn fn_creator(&self) -> FunctionCreator;
}

pub struct DefaultStreamOperator<T>
where
    T: ?Sized + NamedFunction,
{
    parallelism: u16,
    fn_creator: FunctionCreator,
    pub(crate) operator_fn: Box<T>,
}

impl<T> DefaultStreamOperator<T>
where
    T: ?Sized + NamedFunction,
{
    pub fn new(parallelism: u16, fn_creator: FunctionCreator, operator_fn: Box<T>) -> Self {
        DefaultStreamOperator {
            parallelism,
            fn_creator,
            operator_fn,
        }
    }
}

impl<T> TStreamOperator for DefaultStreamOperator<T>
where
    T: ?Sized + NamedFunction,
{
    fn operator_name(&self) -> &str {
        self.operator_fn.name()
    }

    fn parallelism(&self) -> u16 {
        self.parallelism
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        unimplemented!()
    }

    fn fn_creator(&self) -> FunctionCreator {
        self.fn_creator.clone()
    }
}

impl<T> Debug for DefaultStreamOperator<T>
where
    T: ?Sized + NamedFunction,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamOperator")
            .field("parallelism", &self.parallelism)
            .field("fn_creator", &self.fn_creator)
            .field("operator_fn", &self.operator_fn.name())
            .finish()
    }
}

#[derive(Debug)]
pub(crate) enum StreamOperator {
    StreamSource(DefaultStreamOperator<dyn InputFormat>),
    StreamFlatMap(DefaultStreamOperator<dyn FlatMapFunction>),
    StreamFilter(DefaultStreamOperator<dyn FilterFunction>),
    StreamCoProcess(DefaultStreamOperator<dyn CoProcessFunction>),
    StreamKeyBy(DefaultStreamOperator<dyn KeySelectorFunction>),
    StreamReduce(DefaultStreamOperator<dyn BaseReduceFunction>),
    StreamWatermarkAssigner(DefaultStreamOperator<dyn WatermarkStrategy>),
    StreamWindowAssigner(DefaultStreamOperator<dyn WindowAssigner>),
    StreamSink(DefaultStreamOperator<dyn OutputFormat>),
}

impl StreamOperator {
    pub fn new_source(
        parallelism: u16,
        fn_creator: FunctionCreator,
        source_fn: Box<dyn InputFormat>,
    ) -> Self {
        let operator = DefaultStreamOperator::new(parallelism, fn_creator, source_fn);
        StreamOperator::StreamSource(operator)
    }

    pub fn new_map(map_fn: Box<dyn FlatMapFunction>) -> Self {
        let operator =
            DefaultStreamOperator::new(DEFAULT_PARALLELISM, FunctionCreator::User, map_fn);
        StreamOperator::StreamFlatMap(operator)
    }

    pub fn new_filter(filter_fn: Box<dyn FilterFunction>) -> Self {
        let operator =
            DefaultStreamOperator::new(DEFAULT_PARALLELISM, FunctionCreator::User, filter_fn);
        StreamOperator::StreamFilter(operator)
    }

    pub fn new_co_process(co_process_fn: Box<dyn CoProcessFunction>) -> Self {
        let operator =
            DefaultStreamOperator::new(DEFAULT_PARALLELISM, FunctionCreator::User, co_process_fn);
        StreamOperator::StreamCoProcess(operator)
    }

    pub fn new_key_by(key_by_fn: Box<dyn KeySelectorFunction>) -> Self {
        let operator =
            DefaultStreamOperator::new(DEFAULT_PARALLELISM, FunctionCreator::User, key_by_fn);
        StreamOperator::StreamKeyBy(operator)
    }

    pub fn new_reduce(parallelism: u16, reduce_fn: Box<dyn BaseReduceFunction>) -> Self {
        let operator = DefaultStreamOperator::new(parallelism, FunctionCreator::User, reduce_fn);
        StreamOperator::StreamReduce(operator)
    }

    pub fn new_watermark_assigner(watermark_assigner: Box<dyn WatermarkStrategy>) -> Self {
        let operator = DefaultStreamOperator::new(
            DEFAULT_PARALLELISM,
            FunctionCreator::User,
            watermark_assigner,
        );
        StreamOperator::StreamWatermarkAssigner(operator)
    }

    pub fn new_window_assigner(window_assigner: Box<dyn WindowAssigner>) -> Self {
        let operator =
            DefaultStreamOperator::new(DEFAULT_PARALLELISM, FunctionCreator::User, window_assigner);
        StreamOperator::StreamWindowAssigner(operator)
    }

    pub fn new_sink(fn_creator: FunctionCreator, sink_fn: Box<dyn OutputFormat>) -> Self {
        let operator = DefaultStreamOperator::new(DEFAULT_PARALLELISM, fn_creator, sink_fn);
        StreamOperator::StreamSink(operator)
    }

    pub fn is_daemon(&self) -> bool {
        if let StreamOperator::StreamSource(stream_source) = self {
            stream_source.operator_fn.daemon()
        } else {
            false
        }
    }

    pub fn is_source(&self) -> bool {
        if let StreamOperator::StreamSource(_stream_source) = self {
            return true;
        }
        false
    }

    #[allow(dead_code)]
    pub fn is_window(&self) -> bool {
        if let StreamOperator::StreamWindowAssigner(_stream_window) = self {
            return true;
        }
        false
    }

    #[allow(dead_code)]
    pub fn is_key_by(&self) -> bool {
        if let StreamOperator::StreamKeyBy(_stream_key_by) = self {
            return true;
        }
        false
    }

    #[allow(dead_code)]
    pub fn is_reduce(&self) -> bool {
        if let StreamOperator::StreamReduce(_stream_reduce) = self {
            return true;
        }
        false
    }

    #[allow(dead_code)]
    pub fn is_sink(&self) -> bool {
        if let StreamOperator::StreamSink(_stream_sink) = self {
            return true;
        }
        false
    }

    #[allow(dead_code)]
    pub fn is_map(&self) -> bool {
        if let StreamOperator::StreamFlatMap(_stream_map) = self {
            return true;
        }
        false
    }

    #[allow(dead_code)]
    pub fn is_filter(&self) -> bool {
        if let StreamOperator::StreamFilter(_stream_filter) = self {
            return true;
        }
        false
    }

    #[allow(dead_code)]
    pub fn is_connect(&self) -> bool {
        if let StreamOperator::StreamCoProcess(_stream_co_process) = self {
            return true;
        }
        false
    }
}

impl TStreamOperator for StreamOperator {
    fn operator_name(&self) -> &str {
        match self {
            StreamOperator::StreamSource(op) => op.operator_name(),
            StreamOperator::StreamFlatMap(op) => op.operator_name(),
            StreamOperator::StreamFilter(op) => op.operator_name(),
            StreamOperator::StreamCoProcess(op) => op.operator_name(),
            StreamOperator::StreamKeyBy(op) => op.operator_name(),
            StreamOperator::StreamReduce(op) => op.operator_name(),
            StreamOperator::StreamWatermarkAssigner(op) => op.operator_name(),
            StreamOperator::StreamWindowAssigner(op) => op.operator_name(),
            StreamOperator::StreamSink(op) => op.operator_name(),
        }
    }

    fn parallelism(&self) -> u16 {
        match self {
            StreamOperator::StreamSource(op) => op.parallelism(),
            StreamOperator::StreamFlatMap(op) => op.parallelism(),
            StreamOperator::StreamFilter(op) => op.parallelism(),
            StreamOperator::StreamCoProcess(op) => op.parallelism(),
            StreamOperator::StreamKeyBy(op) => op.parallelism(),
            StreamOperator::StreamReduce(op) => op.parallelism(),
            StreamOperator::StreamWatermarkAssigner(op) => op.parallelism(),
            StreamOperator::StreamWindowAssigner(op) => op.parallelism(),
            StreamOperator::StreamSink(op) => op.parallelism(),
        }
    }

    fn schema(&self, input_schema: FnSchema) -> FnSchema {
        match self {
            StreamOperator::StreamSource(op) => op.operator_fn.schema(input_schema),
            StreamOperator::StreamFlatMap(op) => op.operator_fn.schema(input_schema),
            StreamOperator::StreamFilter(_op) => input_schema,
            StreamOperator::StreamCoProcess(op) => op.operator_fn.schema(input_schema),
            StreamOperator::StreamKeyBy(op) => {
                let key_schema = op.operator_fn.key_schema(input_schema.clone());
                FnSchema::Tuple(input_schema.into(), key_schema.into())
            }
            StreamOperator::StreamReduce(op) => {
                let value_schema = op.operator_fn.value_schema(input_schema.clone());

                let schema: Schema = match input_schema {
                    FnSchema::Single(_record_schema) => value_schema.into(),
                    FnSchema::Tuple(_record_schema, mut key_schema) => {
                        let v: Schema = value_schema.into();
                        key_schema.merge(&v); //.unwrap();
                        key_schema
                    }
                    FnSchema::Empty => panic!("unreached!"),
                };

                FnSchema::Tuple(Schema::empty(), schema)
            }
            StreamOperator::StreamWatermarkAssigner(_op) => input_schema,
            StreamOperator::StreamWindowAssigner(_op) => input_schema,
            StreamOperator::StreamSink(op) => op.operator_fn.schema(input_schema),
        }
    }

    fn fn_creator(&self) -> FunctionCreator {
        match self {
            StreamOperator::StreamSource(op) => op.fn_creator(),
            StreamOperator::StreamFlatMap(op) => op.fn_creator(),
            StreamOperator::StreamFilter(op) => op.fn_creator(),
            StreamOperator::StreamCoProcess(op) => op.fn_creator(),
            StreamOperator::StreamKeyBy(op) => op.fn_creator(),
            StreamOperator::StreamReduce(op) => op.fn_creator(),
            StreamOperator::StreamWatermarkAssigner(op) => op.fn_creator(),
            StreamOperator::StreamWindowAssigner(op) => op.fn_creator(),
            StreamOperator::StreamSink(op) => op.fn_creator(),
        }
    }
}
