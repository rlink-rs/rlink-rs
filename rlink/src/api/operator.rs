use std::fmt::Debug;

use crate::api::function::{
    CoProcessFunction, FilterFunction, Function, InputFormat, KeySelectorFunction, MapFunction,
    OutputFormat, ReduceFunction,
};
use crate::api::runtime::OperatorId;
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
    fn get_operator_id(&self) -> OperatorId;
    fn get_parent_operator_id(&self) -> OperatorId;
    fn get_parent_operator_ids(&self) -> Vec<OperatorId>;
    fn get_parallelism(&self) -> u32;
    fn get_fn_creator(&self) -> FunctionCreator;
}

pub struct StreamOperator<T>
where
    T: ?Sized + Function,
{
    id: OperatorId,
    parent_ids: Vec<OperatorId>,
    parallelism: u32,
    fn_creator: FunctionCreator,
    pub(crate) operator_fn: Box<T>,
}

impl<T> StreamOperator<T>
where
    T: ?Sized + Function,
{
    pub fn new(
        id: OperatorId,
        parent_ids: Vec<OperatorId>,
        parallelism: u32,
        fn_creator: FunctionCreator,
        operator_fn: Box<T>,
    ) -> Self {
        StreamOperator {
            id,
            parent_ids,
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

    fn get_operator_id(&self) -> OperatorId {
        self.id
    }

    fn get_parent_operator_id(&self) -> OperatorId {
        self.parent_ids[0]
    }

    fn get_parent_operator_ids(&self) -> Vec<OperatorId> {
        self.parent_ids.clone()
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
        f.debug_struct("BaseStreamOperator")
            .field("id", &self.id)
            .field("parent_id", &self.parent_ids)
            .field("parallelism", &self.parallelism)
            .field("fn_creator", &self.fn_creator)
            .field("operator_fn", &self.operator_fn.get_name())
            .finish()
    }
}

#[derive(Debug)]
pub enum StreamOperatorWrap {
    StreamSource(StreamOperator<dyn InputFormat>),
    StreamMap(StreamOperator<dyn MapFunction>),
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
        id: OperatorId,
        parent_ids: Vec<OperatorId>,
        parallelism: u32,
        fn_creator: FunctionCreator,
        source_fn: Box<dyn InputFormat>,
    ) -> Self {
        let operator = StreamOperator::new(id, parent_ids, parallelism, fn_creator, source_fn);
        StreamOperatorWrap::StreamSource(operator)
    }

    pub fn new_map(
        id: OperatorId,
        parent_ids: Vec<OperatorId>,
        map_fn: Box<dyn MapFunction>,
    ) -> Self {
        let operator = StreamOperator::new(
            id,
            parent_ids,
            DEFAULT_PARALLELISM,
            FunctionCreator::User,
            map_fn,
        );
        StreamOperatorWrap::StreamMap(operator)
    }

    pub fn new_filter(
        id: OperatorId,
        parent_ids: Vec<OperatorId>,
        filter_fn: Box<dyn FilterFunction>,
    ) -> Self {
        let operator = StreamOperator::new(
            id,
            parent_ids,
            DEFAULT_PARALLELISM,
            FunctionCreator::User,
            filter_fn,
        );
        StreamOperatorWrap::StreamFilter(operator)
    }

    pub fn new_co_process(
        id: OperatorId,
        parent_ids: Vec<OperatorId>,
        co_process_fn: Box<dyn CoProcessFunction>,
    ) -> Self {
        let operator = StreamOperator::new(
            id,
            parent_ids,
            DEFAULT_PARALLELISM,
            FunctionCreator::User,
            co_process_fn,
        );
        StreamOperatorWrap::StreamCoProcess(operator)
    }

    pub fn new_key_by(
        id: OperatorId,
        parent_ids: Vec<OperatorId>,
        key_by_fn: Box<dyn KeySelectorFunction>,
    ) -> Self {
        let operator = StreamOperator::new(
            id,
            parent_ids,
            DEFAULT_PARALLELISM,
            FunctionCreator::User,
            key_by_fn,
        );
        StreamOperatorWrap::StreamKeyBy(operator)
    }

    pub fn new_reduce(
        id: OperatorId,
        parent_ids: Vec<OperatorId>,
        parallelism: u32,
        reduce_fn: Box<dyn ReduceFunction>,
    ) -> Self {
        let operator = StreamOperator::new(
            id,
            parent_ids,
            parallelism,
            FunctionCreator::User,
            reduce_fn,
        );
        StreamOperatorWrap::StreamReduce(operator)
    }

    pub fn new_watermark_assigner(
        id: OperatorId,
        parent_ids: Vec<OperatorId>,
        watermark_assigner: Box<dyn WatermarkAssigner>,
    ) -> Self {
        let operator = StreamOperator::new(
            id,
            parent_ids,
            DEFAULT_PARALLELISM,
            FunctionCreator::User,
            watermark_assigner,
        );
        StreamOperatorWrap::StreamWatermarkAssigner(operator)
    }

    pub fn new_window_assigner(
        id: OperatorId,
        parent_ids: Vec<OperatorId>,
        window_assigner: Box<dyn WindowAssigner>,
    ) -> Self {
        let operator = StreamOperator::new(
            id,
            parent_ids,
            DEFAULT_PARALLELISM,
            FunctionCreator::User,
            window_assigner,
        );
        StreamOperatorWrap::StreamWindowAssigner(operator)
    }

    pub fn new_sink(
        id: OperatorId,
        parent_ids: Vec<OperatorId>,
        fn_creator: FunctionCreator,
        sink_fn: Box<dyn OutputFormat>,
    ) -> Self {
        let operator =
            StreamOperator::new(id, parent_ids, DEFAULT_PARALLELISM, fn_creator, sink_fn);
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

    fn get_operator_id(&self) -> OperatorId {
        match self {
            StreamOperatorWrap::StreamSource(op) => op.get_operator_id(),
            StreamOperatorWrap::StreamMap(op) => op.get_operator_id(),
            StreamOperatorWrap::StreamFilter(op) => op.get_operator_id(),
            StreamOperatorWrap::StreamCoProcess(op) => op.get_operator_id(),
            StreamOperatorWrap::StreamKeyBy(op) => op.get_operator_id(),
            StreamOperatorWrap::StreamReduce(op) => op.get_operator_id(),
            StreamOperatorWrap::StreamWatermarkAssigner(op) => op.get_operator_id(),
            StreamOperatorWrap::StreamWindowAssigner(op) => op.get_operator_id(),
            StreamOperatorWrap::StreamSink(op) => op.get_operator_id(),
        }
    }

    fn get_parent_operator_id(&self) -> OperatorId {
        match self {
            StreamOperatorWrap::StreamSource(op) => op.get_parent_operator_id(),
            StreamOperatorWrap::StreamMap(op) => op.get_parent_operator_id(),
            StreamOperatorWrap::StreamFilter(op) => op.get_parent_operator_id(),
            StreamOperatorWrap::StreamCoProcess(op) => op.get_parent_operator_id(),
            StreamOperatorWrap::StreamKeyBy(op) => op.get_parent_operator_id(),
            StreamOperatorWrap::StreamReduce(op) => op.get_parent_operator_id(),
            StreamOperatorWrap::StreamWatermarkAssigner(op) => op.get_parent_operator_id(),
            StreamOperatorWrap::StreamWindowAssigner(op) => op.get_parent_operator_id(),
            StreamOperatorWrap::StreamSink(op) => op.get_parent_operator_id(),
        }
    }

    fn get_parent_operator_ids(&self) -> Vec<OperatorId> {
        match self {
            StreamOperatorWrap::StreamSource(op) => op.get_parent_operator_ids(),
            StreamOperatorWrap::StreamMap(op) => op.get_parent_operator_ids(),
            StreamOperatorWrap::StreamFilter(op) => op.get_parent_operator_ids(),
            StreamOperatorWrap::StreamCoProcess(op) => op.get_parent_operator_ids(),
            StreamOperatorWrap::StreamKeyBy(op) => op.get_parent_operator_ids(),
            StreamOperatorWrap::StreamReduce(op) => op.get_parent_operator_ids(),
            StreamOperatorWrap::StreamWatermarkAssigner(op) => op.get_parent_operator_ids(),
            StreamOperatorWrap::StreamWindowAssigner(op) => op.get_parent_operator_ids(),
            StreamOperatorWrap::StreamSink(op) => op.get_parent_operator_ids(),
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
