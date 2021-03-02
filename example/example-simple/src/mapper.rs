use rlink::api;
use rlink::api::element::Record;
use rlink::api::function::{Context, FilterFunction};

#[derive(Debug, Function)]
pub struct MyFilterFunction {}

impl MyFilterFunction {
    pub fn new() -> Self {
        MyFilterFunction {}
    }
}

impl FilterFunction for MyFilterFunction {
    fn open(&mut self, _context: &Context) -> api::Result<()> {
        Ok(())
    }

    fn filter(&self, _t: &mut Record) -> bool {
        true
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
}
