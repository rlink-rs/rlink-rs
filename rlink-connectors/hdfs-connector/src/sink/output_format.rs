use rlink::api::element::Record;
use rlink::api::function::{Context, OutputFormat};

#[derive(Function)]
pub struct HdfsOutputFormat {}

impl OutputFormat for HdfsOutputFormat {
    fn open(&mut self, _context: &Context) -> rlink::api::Result<()> {
        Ok(())
    }

    fn write_record(&mut self, _record: Record) {
        unimplemented!()
    }

    fn close(&mut self) -> rlink::api::Result<()> {
        Ok(())
    }
}
