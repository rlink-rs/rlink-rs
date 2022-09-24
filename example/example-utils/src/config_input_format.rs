use futures::Stream;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use tokio::time::Interval;

use rlink::core;
use rlink::core::element::{Element, FnSchema, Record};
use rlink::core::function::{
    Context, ElementStream, InputFormat, InputSplit, InputSplitSource, SendableElementStream,
};

use crate::buffer_gen::config;

#[derive(Debug, Function)]
pub struct ConfigInputFormat {
    name: &'static str,
}

impl ConfigInputFormat {
    pub fn new(name: &'static str) -> Self {
        ConfigInputFormat { name }
    }

    fn gen_row(&self) -> Vec<Record> {
        let mut rows = Vec::new();

        rows.push(self.create_record("0"));
        rows.push(self.create_record("1"));
        rows.push(self.create_record("2"));
        rows.push(self.create_record("3"));
        rows.push(self.create_record("4"));

        rows
    }

    fn create_record(&self, value: &str) -> Record {
        let field = format!("{}-{}", self.name, value);
        let model = config::Entity {
            field: field.as_str(),
            value: value,
        };
        let mut record = Record::new();
        model.to_buffer(record.as_buffer()).unwrap();

        record
    }
}

impl InputSplitSource for ConfigInputFormat {}

#[async_trait]
impl InputFormat for ConfigInputFormat {
    async fn open(&mut self, _input_split: InputSplit, _context: &Context) -> core::Result<()> {
        Ok(())
    }

    async fn element_stream(&mut self) -> SendableElementStream {
        Box::pin(IntervalConfigStream::new(
            Duration::from_secs(1),
            self.gen_row().into_iter(),
        ))
    }

    async fn close(&mut self) -> core::Result<()> {
        Ok(())
    }

    fn daemon(&self) -> bool {
        true
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::from(&config::FIELD_METADATA)
    }

    fn parallelism(&self) -> u16 {
        1
    }
}

pub struct IntervalConfigStream {
    inner: Interval,
    conf: std::vec::IntoIter<Record>,
}

impl IntervalConfigStream {
    pub fn new(period: Duration, conf: std::vec::IntoIter<Record>) -> Self {
        let interval = tokio::time::interval(period);
        Self {
            inner: interval,
            conf,
        }
    }
}

impl ElementStream for IntervalConfigStream {}

impl Stream for IntervalConfigStream {
    type Item = Element;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner
            .poll_tick(cx)
            .map(|_| self.conf.next().map(|record| Element::Record(record)))
    }
}
