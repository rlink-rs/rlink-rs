use rlink::api::element::Record;
use rlink::api::function::{Context, OutputFormat};

use crate::writer::BlockWriterManager;

#[derive(Function)]
pub struct HdfsOutputFormat {
    writer_manager: Box<dyn BlockWriterManager>,
}

impl OutputFormat for HdfsOutputFormat {
    fn open(&mut self, _context: &Context) -> rlink::api::Result<()> {
        Ok(())
    }

    fn write_record(&mut self, record: Record) {
        self.writer_manager.append(record).unwrap();
    }

    fn close(&mut self) -> rlink::api::Result<()> {
        self.writer_manager.close()?;
        Ok(())
    }
}
