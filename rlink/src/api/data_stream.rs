use std::fmt::Debug;

use crate::api::function::{
    FilterFunction, InputFormat, KeySelectorFunction, MapFunction, OutputFormat, ReduceFunction,
};
use crate::api::operator::{FunctionCreator, StreamOperatorWrap};
use crate::api::watermark::WatermarkAssigner;
use crate::api::window::WindowAssigner;

pub(crate) const ROOT_ID: u32 = 100;

pub(crate) trait StreamGraph: Debug {
    fn into_operators(self) -> Vec<StreamOperatorWrap>;
}

pub trait TDataStream {
    fn map<F>(self, map: F) -> DataStream
    where
        F: MapFunction + 'static;

    fn filter<F>(self, filter: F) -> DataStream
    where
        F: FilterFunction + 'static;

    fn key_by<F>(self, key_by: F) -> KeyedStream
    where
        F: KeySelectorFunction + 'static;

    fn assign_timestamps_and_watermarks<W>(self, timestamp_and_watermark_assigner: W) -> DataStream
    where
        W: WatermarkAssigner + 'static;

    fn add_sink<O>(self, output_format: O) -> SinkStream
    where
        O: OutputFormat + 'static;
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

#[derive(Debug)]
pub enum DataStream {
    DefaultDataStream(DataStreamSource),
}

impl TDataStream for DataStream {
    fn map<F>(self, map: F) -> DataStream
    where
        F: MapFunction + 'static,
    {
        match self {
            DataStream::DefaultDataStream(data_stream) => data_stream.map(map),
        }
    }

    fn filter<F>(self, filter: F) -> DataStream
    where
        F: FilterFunction + 'static,
    {
        match self {
            DataStream::DefaultDataStream(data_stream) => data_stream.filter(filter),
        }
    }

    fn key_by<F>(self, key_by: F) -> KeyedStream
    where
        F: KeySelectorFunction + 'static,
    {
        match self {
            DataStream::DefaultDataStream(data_stream) => data_stream.key_by(key_by),
        }
    }

    fn assign_timestamps_and_watermarks<W>(self, timestamp_and_watermark_assigner: W) -> DataStream
    where
        W: WatermarkAssigner + 'static,
    {
        match self {
            DataStream::DefaultDataStream(data_stream) => {
                data_stream.assign_timestamps_and_watermarks(timestamp_and_watermark_assigner)
            }
        }
    }

    fn add_sink<O>(self, output_format: O) -> SinkStream
    where
        O: OutputFormat + 'static,
    {
        match self {
            DataStream::DefaultDataStream(data_stream) => {
                TDataStream::add_sink(data_stream, output_format)
            }
        }
    }
}

#[derive(Debug)]
pub enum KeyedStream {
    DefaultKeyedStream(DataStreamSource),
}

impl TKeyedStream for KeyedStream {
    fn window<W>(self, window_assigner: W) -> WindowedStream
    where
        W: WindowAssigner + 'static,
    {
        match self {
            KeyedStream::DefaultKeyedStream(keyed_stream) => keyed_stream.window(window_assigner),
        }
    }

    fn add_sink<O>(self, output_format: O) -> SinkStream
    where
        O: OutputFormat + 'static,
    {
        match self {
            KeyedStream::DefaultKeyedStream(keyed_stream) => {
                TKeyedStream::add_sink(keyed_stream, output_format)
            }
        }
    }
}

#[derive(Debug)]
pub enum WindowedStream {
    DefaultWindowedStream(DataStreamSource),
}

impl TWindowedStream for WindowedStream {
    fn reduce<F>(self, reduce: F, parallelism: u32) -> DataStream
    where
        F: ReduceFunction + 'static,
    {
        match self {
            WindowedStream::DefaultWindowedStream(windowed_stream) => {
                windowed_stream.reduce(reduce, parallelism)
            }
        }
    }
}

#[derive(Debug)]
pub enum SinkStream {
    DefaultEndStream(DataStreamSource),
}

impl TEndStream for SinkStream {}

impl StreamGraph for SinkStream {
    fn into_operators(self) -> Vec<StreamOperatorWrap> {
        match self {
            SinkStream::DefaultEndStream(end_stream) => end_stream.into_operators(),
        }
    }
}

#[derive(Debug)]
pub struct DataStreamSource {
    current_id: u32,
    operators: Vec<StreamOperatorWrap>,
}

impl DataStreamSource {
    pub fn new(source_func: Box<dyn InputFormat>, parallelism: u32) -> Self {
        let parent_id = ROOT_ID;
        let id = parent_id + 1;
        let source_operator = StreamOperatorWrap::new_source(
            id,
            parent_id,
            parallelism,
            FunctionCreator::User,
            source_func,
        );

        DataStreamSource {
            current_id: id,
            operators: vec![source_operator],
        }
    }
}

impl StreamGraph for DataStreamSource {
    fn into_operators(self) -> Vec<StreamOperatorWrap> {
        self.operators
    }
}

impl TDataStream for DataStreamSource {
    fn map<F>(mut self, map: F) -> DataStream
    where
        F: MapFunction + 'static,
    {
        let parent_id = self.current_id;
        self.current_id += 1;
        let id = self.current_id;

        let map_func = Box::new(map);
        let stream_map = StreamOperatorWrap::new_map(id, parent_id, map_func);

        self.operators.push(stream_map);

        DataStream::DefaultDataStream(self)
    }

    fn filter<F>(mut self, filter: F) -> DataStream
    where
        F: FilterFunction + 'static,
    {
        let parent_id = self.current_id;
        self.current_id += 1;
        let id = self.current_id;

        let filter_func = Box::new(filter);
        let stream_filter = StreamOperatorWrap::new_filter(id, parent_id, filter_func);

        self.operators.push(stream_filter);

        DataStream::DefaultDataStream(self)
    }

    fn key_by<F>(mut self, key_by: F) -> KeyedStream
    where
        F: KeySelectorFunction + 'static,
    {
        let parent_id = self.current_id;
        self.current_id += 1;
        let id = self.current_id;

        let key_by_func = Box::new(key_by);
        let stream_key_by = StreamOperatorWrap::new_key_by(id, parent_id, key_by_func);

        self.operators.push(stream_key_by);

        KeyedStream::DefaultKeyedStream(self)
    }

    fn assign_timestamps_and_watermarks<W>(
        mut self,
        timestamp_and_watermark_assigner: W,
    ) -> DataStream
    where
        W: WatermarkAssigner + 'static,
    {
        let parent_id = self.current_id;
        self.current_id += 1;
        let id = self.current_id;

        let time_assigner_func = Box::new(timestamp_and_watermark_assigner);
        let stream_watermark_assigner =
            StreamOperatorWrap::new_watermark_assigner(id, parent_id, time_assigner_func);

        self.operators.push(stream_watermark_assigner);

        DataStream::DefaultDataStream(self)
    }

    fn add_sink<O>(mut self, output_format: O) -> SinkStream
    where
        O: OutputFormat + 'static,
    {
        let parent_id = self.current_id;
        self.current_id += 1;
        let id = self.current_id;

        let sink_func = Box::new(output_format);
        let stream_sink =
            StreamOperatorWrap::new_sink(id, parent_id, FunctionCreator::User, sink_func);

        self.operators.push(stream_sink);

        SinkStream::DefaultEndStream(self)
    }
}

impl TKeyedStream for DataStreamSource {
    fn window<W>(mut self, window_assigner: W) -> WindowedStream
    where
        W: WindowAssigner + 'static,
    {
        let parent_id = self.current_id;
        self.current_id += 1;
        let id = self.current_id;

        let window_assigner_func = Box::new(window_assigner);
        let stream_window_assigner =
            StreamOperatorWrap::new_window_assigner(id, parent_id, window_assigner_func);

        self.operators.push(stream_window_assigner);

        WindowedStream::DefaultWindowedStream(self)
    }

    fn add_sink<O>(mut self, output_format: O) -> SinkStream
    where
        O: OutputFormat + 'static,
    {
        let parent_id = self.current_id;
        self.current_id += 1;
        let id = self.current_id;

        let sink_func = Box::new(output_format);
        let stream_sink =
            StreamOperatorWrap::new_sink(id, parent_id, FunctionCreator::User, sink_func);

        self.operators.push(stream_sink);

        SinkStream::DefaultEndStream(self)
    }
}

impl TWindowedStream for DataStreamSource {
    fn reduce<F>(mut self, reduce: F, parallelism: u32) -> DataStream
    where
        F: ReduceFunction + 'static,
    {
        let parent_id = self.current_id;
        self.current_id += 1;
        let id = self.current_id;

        let reduce_func = Box::new(reduce);
        let stream_reduce = StreamOperatorWrap::new_reduce(id, parent_id, parallelism, reduce_func);

        self.operators.push(stream_reduce);

        DataStream::DefaultDataStream(self)
    }
}

impl TEndStream for DataStreamSource {}

#[cfg(test)]
mod tests {
    use crate::api::data_stream::{DataStream, TDataStream, TWindowedStream};
    use crate::api::data_stream::{DataStreamSource, TKeyedStream};
    use crate::api::element::Record;
    use crate::api::function::{
        Context, Function, InputFormat, InputSplit, InputSplitAssigner, InputSplitSource,
        KeySelectorFunction, MapFunction, OutputFormat, ReduceFunction,
    };
    use crate::api::properties::Properties;
    use crate::api::watermark::{BoundedOutOfOrdernessTimestampExtractor, TimestampAssigner};
    use crate::api::window::SlidingEventTimeWindows;
    use std::time::Duration;

    #[test]
    pub fn data_stream_test() {
        let n = DataStreamSource::new(Box::new(MyInputFormat::new()), 10);
        let data_stream = DataStream::DefaultDataStream(n);

        let end_stream = data_stream
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

        println!("{:?}", end_stream);
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct MyInputFormat {}

    impl MyInputFormat {
        pub fn new() -> Self {
            MyInputFormat {}
        }
    }

    impl InputSplitSource for MyInputFormat {
        fn create_input_splits(&self, _min_num_splits: u32) -> Vec<InputSplit> {
            vec![]
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
}
