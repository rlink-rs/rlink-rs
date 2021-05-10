use rlink::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use rlink::core::element::Record;
use rlink::core::function::{Context, OutputFormat};
use rlink::core::runtime::TaskId;

use crate::writer::BlockWriterManager;

#[derive(NamedFunction)]
pub struct HdfsOutputFormat {
    task_id: Option<TaskId>,
    writer_manager: Box<dyn BlockWriterManager>,
}

impl HdfsOutputFormat {
    pub fn new(writer_manager: Box<dyn BlockWriterManager>) -> Self {
        HdfsOutputFormat {
            task_id: None,
            writer_manager,
        }
    }
}

impl OutputFormat for HdfsOutputFormat {
    fn open(&mut self, context: &Context) -> rlink::core::Result<()> {
        self.task_id = Some(context.task_id.clone());
        self.writer_manager.open()?;
        Ok(())
    }

    fn write_record(&mut self, record: Record) {
        self.writer_manager
            .append(record, self.task_id.as_ref().unwrap())
            .unwrap();
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        self.writer_manager.close()?;
        Ok(())
    }
}

impl CheckpointFunction for HdfsOutputFormat {
    fn snapshot_state(&mut self, _context: &FunctionSnapshotContext) -> Option<CheckpointHandle> {
        self.writer_manager.snapshot().unwrap();
        None
    }
}
