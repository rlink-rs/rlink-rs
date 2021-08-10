use rlink::core;
use rlink::core::element::{Record, Schema};
use rlink::core::function::{CoProcessFunction, Context};
use rlink_example_utils::buffer_gen::{config, model};

#[derive(Debug, Function)]
pub struct MyCoProcessFunction {}

impl CoProcessFunction for MyCoProcessFunction {
    fn open(&mut self, _context: &Context) -> core::Result<()> {
        Ok(())
    }

    fn process_left(&mut self, record: Record) -> Box<dyn Iterator<Item = Record>> {
        // let n = model::Entity::parse(record.as_buffer()).unwrap();
        // info!("--> {:?}", n);
        Box::new(vec![record].into_iter())
    }

    fn process_right(
        &mut self,
        stream_seq: usize,
        mut record: Record,
    ) -> Box<dyn Iterator<Item = Record>> {
        let conf = config::Entity::parse(record.as_buffer()).unwrap();
        info!(
            "Right Stream: {}, config [field:{}, val:{}]",
            stream_seq, conf.field, conf.value
        );

        Box::new(vec![].into_iter())
    }

    fn close(&mut self) -> core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: Schema) -> Schema {
        Schema::from(&model::FIELD_TYPE[..])
    }
}
