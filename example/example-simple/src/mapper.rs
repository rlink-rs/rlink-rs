use rlink::core;
use rlink::core::element::Record;
use rlink::core::function::{Context, FilterFunction};

#[derive(Debug, Function)]
pub struct MyFilterFunction {}

impl MyFilterFunction {
    pub fn new() -> Self {
        MyFilterFunction {}
    }
}

impl FilterFunction for MyFilterFunction {
    fn open(&mut self, _context: &Context) -> core::Result<()> {
        Ok(())
    }

    fn filter(&self, _t: &mut Record) -> bool {
        true
    }

    fn close(&mut self) -> core::Result<()> {
        Ok(())
    }
}
