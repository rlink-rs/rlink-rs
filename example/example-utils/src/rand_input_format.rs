use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use futures::Stream;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rlink::core;
use rlink::core::element::{Element, FnSchema, Record};
use rlink::core::function::{
    Context, ElementStream, InputFormat, InputSplit, InputSplitSource, SendableElementStream,
};
use rlink::utils::date_time::current_timestamp_millis;
use tokio::time::Interval;

use crate::buffer_gen::model;

#[derive(Debug, Function)]
pub struct RandInputFormat {
    parallelism: u16,
}

impl RandInputFormat {
    pub fn new(parallelism: u16) -> Self {
        RandInputFormat { parallelism }
    }
}

impl InputSplitSource for RandInputFormat {}

#[async_trait]
impl InputFormat for RandInputFormat {
    async fn open(&mut self, _input_split: InputSplit, _context: &Context) -> core::Result<()> {
        Ok(())
    }

    async fn element_stream(&mut self) -> SendableElementStream {
        Box::pin(IntervalRandStream::new())
    }

    async fn close(&mut self) -> core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::from(&model::FIELD_METADATA)
    }

    fn parallelism(&self) -> u16 {
        self.parallelism
    }
}

// struct RandIterator {
//     receiver: Receiver<Record>,
// }
//
// impl RandIterator {
//     pub fn new() -> Self {
//         let (sender, receiver) = channel();
//         Self::rand_record(sender);
//         RandIterator { receiver }
//     }
//
//     fn rand_record(sender: Sender<Record>) {
//         loop {
//             std::thread::sleep(Duration::from_millis(10));
//
//             let mut rng: StdRng = SeedableRng::from_entropy();
//             let v = rng.gen_range(0i32..100i32) as i64;
//
//             let name = format!("name-{}", v);
//             let model = model::Entity {
//                 timestamp: current_timestamp_millis(),
//                 name: name.as_str(),
//                 value: v,
//             };
//
//             let mut record = Record::new();
//             model.to_buffer(record.as_buffer()).unwrap();
//
//             match sender.send(record) {
//                 Ok(()) => {}
//                 Err(_e) => println!("rand buffer is full"),
//             }
//         }
//     }
// }
//
// impl Iterator for RandIterator {
//     type Item = Record;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if let Ok(record) = self.receiver.try_recv() {}
//         std::thread::sleep(Duration::from_millis(10));
//
//         let mut rng: StdRng = SeedableRng::from_entropy();
//         let v = rng.gen_range(0i32..100i32) as i64;
//
//         let name = format!("name-{}", v);
//         let model = model::Entity {
//             timestamp: current_timestamp_millis(),
//             name: name.as_str(),
//             value: v,
//         };
//
//         let mut record = Record::new();
//         model.to_buffer(record.as_buffer()).unwrap();
//
//         Some(record)
//     }
// }

pub struct IntervalRandStream {
    inner: Interval,
}

impl IntervalRandStream {
    pub fn new() -> Self {
        let interval = tokio::time::interval(Duration::from_millis(10));
        Self { inner: interval }
    }

    pub fn with_period(period: Duration) -> Self {
        let interval = tokio::time::interval(period);
        Self { inner: interval }
    }
}

impl ElementStream for IntervalRandStream {}

impl Stream for IntervalRandStream {
    type Item = Element;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.poll_tick(cx).map(|_| {
            let mut rng: StdRng = SeedableRng::from_entropy();
            let v = rng.gen_range(0i32..100i32) as i64;

            let name = format!("name-{}", v);
            let model = model::Entity {
                timestamp: current_timestamp_millis(),
                name: name.as_str(),
                value: v,
            };

            let mut record = Record::new();
            model.to_buffer(record.as_buffer()).unwrap();

            Some(Element::Record(record))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;

    use crate::rand_input_format::IntervalRandStream;

    #[tokio::test]
    pub async fn interval_rand_stream_test() {
        let mut stream = IntervalRandStream::with_period(Duration::from_secs(1));
        while let Some(element) = stream.next().await {
            println!("{:?}", element);
        }
    }
}
