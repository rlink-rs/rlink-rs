use rlink::core;
use rlink::core::element::Record;
use rlink::core::function::{Context, FlatMapFunction};

#[derive(Debug, Function)]
pub struct MyFlatMapFunction {}

impl MyFlatMapFunction {
    pub fn new() -> Self {
        MyFlatMapFunction {}
    }
}

impl FlatMapFunction for MyFlatMapFunction {
    fn open(&mut self, _context: &Context) -> core::Result<()> {
        Ok(())
    }

    fn flat_map(&mut self, record: Record) -> Box<dyn Iterator<Item = Record>> {
        Box::new(vec![record].into_iter())
    }

    fn close(&mut self) -> core::Result<()> {
        Ok(())
    }
}
