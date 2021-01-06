use std::fmt::Debug;

use crate::api::env_v2::StreamManager;
use crate::api::function::{
    CoProcessFunction, FilterFunction, InputFormat, KeySelectorFunction, MapFunction, OutputFormat,
    ReduceFunction,
};
use crate::api::operator::{FunctionCreator, StreamOperatorWrap};
use crate::api::watermark::WatermarkAssigner;
use crate::api::window::WindowAssigner;
use crate::dag::stream_graph::OperatorId;
use std::rc::Rc;

pub(crate) trait PipelineStream: Debug {
    fn into_operators(self) -> Vec<StreamOperatorWrap>;
}

pub trait TDataStream {
    fn map<F>(self, map: F) -> DataStream
    where
        F: MapFunction + 'static;

    fn filter<F>(self, filter: F) -> DataStream
    where
        F: FilterFunction + 'static;

    fn key_by<F>(self, key_selector: F) -> KeyedStream
    where
        F: KeySelectorFunction + 'static;

    fn assign_timestamps_and_watermarks<W>(self, timestamp_and_watermark_assigner: W) -> DataStream
    where
        W: WatermarkAssigner + 'static;

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

pub trait TEndStream {}

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
    fn map<F>(self, mapper: F) -> DataStream
    where
        F: MapFunction + 'static,
    {
        self.data_stream.map(mapper)
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
    dependency_pipeline_ids: Vec<u32>,
}

impl ConnectedStreams {
    pub(crate) fn new(co_stream: StreamBuilder, dependency_pipeline_ids: Vec<u32>) -> Self {
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

impl TEndStream for SinkStream {}

impl PipelineStream for SinkStream {
    fn into_operators(self) -> Vec<StreamOperatorWrap> {
        self.end_stream.into_operators()
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
        let source_operator = StreamOperatorWrap::new_source(
            0,
            vec![],
            parallelism,
            FunctionCreator::User,
            source_func,
        );
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
        parent_ids: Vec<u32>,
    ) -> Self {
        let co_operator = StreamOperatorWrap::new_co_process(0, vec![], co_process_func);
        let operator_id = stream_manager.add_operator(co_operator, parent_ids);

        StreamBuilder {
            current_id: 0,
            cur_operator_id: operator_id,
            stream_manager,
        }
    }
}

impl PipelineStream for StreamBuilder {
    fn into_operators(self) -> Vec<StreamOperatorWrap> {
        unimplemented!()
    }
}

impl TDataStream for StreamBuilder {
    fn map<F>(mut self, mapper: F) -> DataStream
    where
        F: MapFunction + 'static,
    {
        let map_func = Box::new(mapper);
        let stream_map = StreamOperatorWrap::new_map(0, vec![], map_func);

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
        let stream_filter = StreamOperatorWrap::new_filter(0, vec![], filter_func);

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
        let stream_key_by = StreamOperatorWrap::new_key_by(0, vec![], key_selector_func);

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
            StreamOperatorWrap::new_watermark_assigner(0, vec![], time_assigner_func);

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

        let mut dependency_streams: Vec<StreamBuilder> =
            data_streams.into_iter().map(|x| x.data_stream).collect();
        dependency_streams.push(self);

        let parent_ids: Vec<u32> = dependency_streams
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
        let stream_sink = StreamOperatorWrap::new_sink(0, vec![], FunctionCreator::User, sink_func);

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
        let stream_window_assigner =
            StreamOperatorWrap::new_window_assigner(0, vec![], window_assigner_func);

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
        let stream_sink = StreamOperatorWrap::new_sink(0, vec![], FunctionCreator::User, sink_func);

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
        let stream_reduce = StreamOperatorWrap::new_reduce(0, vec![], parallelism, reduce_func);

        self.cur_operator_id = self
            .stream_manager
            .add_operator(stream_reduce, vec![self.cur_operator_id]);

        DataStream::new(self)
    }
}

impl TEndStream for StreamBuilder {}

#[cfg(test)]
mod tests {
    use std::borrow::BorrowMut;
    use std::ops::Deref;
    use std::time::Duration;

    use crate::api::data_stream_v2::{TConnectedStreams, TKeyedStream};
    use crate::api::data_stream_v2::{TDataStream, TWindowedStream};
    use crate::api::element::Record;
    use crate::api::env_v2::StreamExecutionEnvironment;
    use crate::api::function::{
        CoProcessFunction, Context, Function, InputFormat, InputSplit, InputSplitAssigner,
        InputSplitSource, KeySelectorFunction, MapFunction, OutputFormat, ReduceFunction,
    };
    use crate::api::properties::Properties;
    use crate::api::watermark::{BoundedOutOfOrdernessTimestampExtractor, TimestampAssigner};
    use crate::api::window::SlidingEventTimeWindows;
    use crate::dag::execution_graph::ExecutionGraph;
    use crate::dag::job_graph::JobGraph;
    use crate::dag::JsonDag;

    #[test]
    pub fn data_stream_test() {
        let mut env = StreamExecutionEnvironment::new("job_name".to_string());

        env.register_source(MyInputFormat::new(), 100)
            .map(MyMapFunction::new())
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(1),
                MyTimestampAssigner::new(),
            ))
            .key_by(MyKeySelectorFunction::new())
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(20),
                None,
            ))
            .reduce(MyReduceFunction::new(), 10)
            .add_sink(MyOutputFormat::new(Properties::new()));

        println!("{:?}", env.stream_manager.stream_graph.borrow().dag);
    }

    #[test]
    pub fn data_stream_connect_test() {
        let mut env = StreamExecutionEnvironment::new("job_name".to_string());

        let ds = env
            .register_source(MyInputFormat::new(), 1)
            .map(MyMapFunction::new())
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(1),
                MyTimestampAssigner::new(),
            ));

        env.register_source(MyInputFormat::new(), 2)
            .map(MyMapFunction::new())
            .assign_timestamps_and_watermarks(BoundedOutOfOrdernessTimestampExtractor::new(
                Duration::from_secs(1),
                MyTimestampAssigner::new(),
            ))
            .connect(vec![ds], MyCoProcessFunction {})
            .key_by(MyKeySelectorFunction::new())
            .window(SlidingEventTimeWindows::new(
                Duration::from_secs(60),
                Duration::from_secs(20),
                None,
            ))
            .reduce(MyReduceFunction::new(), 3)
            .map(MyMapFunction::new())
            .add_sink(MyOutputFormat::new(Properties::new()));

        {
            let dag = &env.stream_manager.stream_graph.borrow().dag;
            println!("{:?}", dag);
            println!(
                "{}",
                JsonDag::dag_json(dag).fill_begin_end_node().to_string()
            )
        }

        let mut job_graph = JobGraph::new();
        job_graph
            .build(env.stream_manager.stream_graph.borrow().deref())
            .unwrap();
        {
            let dag = &job_graph.dag;
            println!("{:?}", dag);
            println!(
                "{}",
                JsonDag::dag_json(dag).fill_begin_end_node().to_string()
            )
        }

        let mut operators = env.stream_manager.pop_operators();
        let mut execution_graph = ExecutionGraph::new();
        execution_graph
            .build(&job_graph, operators.borrow_mut())
            .unwrap();
        {
            let dag = &execution_graph.dag;
            println!("{:?}", dag);
            println!(
                "{}",
                JsonDag::dag_json(dag).fill_begin_end_node().to_string()
            )
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct MyInputFormat {}

    impl MyInputFormat {
        pub fn new() -> Self {
            MyInputFormat {}
        }
    }

    impl InputSplitSource for MyInputFormat {
        fn create_input_splits(&self, min_num_splits: u32) -> Vec<InputSplit> {
            let mut input_splits = Vec::with_capacity(min_num_splits as usize);
            for partition_num in 0..min_num_splits {
                input_splits.push(InputSplit::new(partition_num, Properties::new()));
            }
            input_splits
        }

        fn get_input_split_assigner(&self, input_splits: Vec<InputSplit>) -> InputSplitAssigner {
            InputSplitAssigner::new(input_splits)
        }
    }

    impl Function for MyInputFormat {
        fn get_name(&self) -> &str {
            "MyInputFormat"
        }
    }

    impl InputFormat for MyInputFormat {
        fn open(&mut self, _input_split: InputSplit, _context: &Context) {}

        fn reached_end(&self) -> bool {
            false
        }

        fn next_record(&mut self) -> Option<Record> {
            None
        }

        fn close(&mut self) {}
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct MyMapFunction {}

    impl MyMapFunction {
        pub fn new() -> Self {
            MyMapFunction {}
        }
    }

    impl MapFunction for MyMapFunction {
        fn open(&mut self, _context: &Context) {}

        fn map(&mut self, t: &mut Record) -> Vec<Record> {
            vec![t.clone()]
        }

        fn close(&mut self) {}
    }

    impl Function for MyMapFunction {
        fn get_name(&self) -> &str {
            "MyMapFunction"
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct MyTimestampAssigner {}

    impl MyTimestampAssigner {
        pub fn new() -> Self {
            MyTimestampAssigner {}
        }
    }

    impl TimestampAssigner for MyTimestampAssigner {
        fn extract_timestamp(
            &mut self,
            _row: &mut Record,
            _previous_element_timestamp: u64,
        ) -> u64 {
            0
        }
    }

    impl Function for MyTimestampAssigner {
        fn get_name(&self) -> &str {
            "MyTimestampAssigner"
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct MyKeySelectorFunction {}

    impl MyKeySelectorFunction {
        pub fn new() -> Self {
            MyKeySelectorFunction {}
        }
    }

    impl KeySelectorFunction for MyKeySelectorFunction {
        fn open(&mut self, _context: &Context) {}

        fn get_key(&self, _record: &mut Record) -> Record {
            let record_rt = Record::new();
            record_rt
        }

        fn close(&mut self) {}
    }

    impl Function for MyKeySelectorFunction {
        fn get_name(&self) -> &str {
            "MyKeySelectorFunction"
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct MyReduceFunction {}

    impl MyReduceFunction {
        pub fn new() -> Self {
            MyReduceFunction {}
        }
    }

    impl ReduceFunction for MyReduceFunction {
        fn open(&mut self, _context: &Context) {}

        fn reduce(&self, _state_value: Option<&mut Record>, record: &mut Record) -> Record {
            record.clone()
        }

        fn close(&mut self) {}
    }

    impl Function for MyReduceFunction {
        fn get_name(&self) -> &str {
            "MyReduceFunction"
        }
    }

    #[derive(Debug)]
    pub struct MyOutputFormat {
        properties: Properties,
    }

    impl MyOutputFormat {
        pub fn new(properties: Properties) -> Self {
            MyOutputFormat { properties }
        }
    }

    impl OutputFormat for MyOutputFormat {
        fn open(&mut self, _context: &Context) {}

        fn write_record(&mut self, _record: Record) {}

        fn close(&mut self) {}
    }

    impl Function for MyOutputFormat {
        fn get_name(&self) -> &str {
            "MyOutputFormat"
        }
    }

    pub struct MyCoProcessFunction {}

    impl CoProcessFunction for MyCoProcessFunction {
        fn open(&mut self, _context: &Context) {}

        fn process(&self, _stream_seq: usize, record: Record) -> Record {
            record
        }

        fn close(&mut self) {}
    }

    impl Function for MyCoProcessFunction {
        fn get_name(&self) -> &str {
            "MyCoProcessFunction"
        }
    }
}
