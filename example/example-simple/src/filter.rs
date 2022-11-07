use rlink::core;
use rlink::core::element::{Element, FnSchema};
use rlink::core::function::{Context, FlatMapFunction, SendableElementStream};
use rlink::utils::stream::MemoryStream;

#[derive(Debug, Function)]
pub struct MyFlatMapFunction {}

impl MyFlatMapFunction {
    pub fn new() -> Self {
        MyFlatMapFunction {}
    }
}

#[async_trait]
impl FlatMapFunction for MyFlatMapFunction {
    async fn open(&mut self, _context: &Context) -> core::Result<()> {
        Ok(())
    }

    async fn flat_map_element(&mut self, element: Element) -> SendableElementStream {
        Box::pin(MemoryStream::new(vec![element.into_record()]))
    }

    async fn close(&mut self) -> core::Result<()> {
        Ok(())
    }

    fn schema(&self, input_schema: FnSchema) -> FnSchema {
        input_schema
    }
}
