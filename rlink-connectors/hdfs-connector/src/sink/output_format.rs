use rlink::api::function::{OutputFormat, Context};
use rlink::api::element::Record;

pub struct HdfsOutputFormat {}

impl OutputFormat for HdfsOutputFormat {
    fn open(&mut self, context: &Context) -> rlink::Result<()> {
        Ok(())
    }

    fn write_record(&mut self, record: Record) {
        unimplemented!()
    }

    fn close(&mut self) -> rlink::Result<()> {
        Ok(())
    }
}