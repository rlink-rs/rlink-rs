use rlink::core;
use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{CoProcessFunction, Context, SendableElementStream};
use rlink::utils::stream::MemoryStream;
use rlink_example_utils::buffer_gen::config;

#[derive(Debug, Function)]
pub struct MyCoProcessFunction {}

#[async_trait]
impl CoProcessFunction for MyCoProcessFunction {
    async fn open(&mut self, _context: &Context) -> core::Result<()> {
        Ok(())
    }

    async fn process_left(&mut self, record: Record) -> SendableElementStream {
        // let n = model::Entity::parse(record.as_buffer()).unwrap();
        // info!("--> {:?}", n);
        Box::pin(MemoryStream::new(vec![record]))
    }

    async fn process_right(
        &mut self,
        stream_seq: usize,
        mut record: Record,
    ) -> SendableElementStream {
        let conf = config::Entity::parse(record.as_buffer()).unwrap();
        info!(
            "Right Stream: {}, config [field:{}, val:{}]",
            stream_seq, conf.field, conf.value
        );

        Box::pin(MemoryStream::new(vec![]))
    }

    async fn close(&mut self) -> core::Result<()> {
        Ok(())
    }

    fn schema(&self, input_schema: FnSchema) -> FnSchema {
        input_schema
    }
}
