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

#[async_trait]
impl FilterFunction for MyFilterFunction {
    async fn open(&mut self, _context: &Context) -> core::Result<()> {
        Ok(())
    }

    async fn filter(&self, _t: &mut Record) -> bool {
        true
    }

    async fn close(&mut self) -> core::Result<()> {
        Ok(())
    }
}
