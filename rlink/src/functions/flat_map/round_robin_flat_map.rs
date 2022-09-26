use crate::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::{Element, FnSchema};
use crate::core::function::{Context, FlatMapFunction, NamedFunction, SendableElementStream};
use crate::utils::stream::MemoryStream;

pub struct RoundRobinFlagMapFunction {
    child_job_parallelism: u16,
    partition_num: u16,
}

impl RoundRobinFlagMapFunction {
    pub fn new() -> Self {
        RoundRobinFlagMapFunction {
            child_job_parallelism: 0,
            partition_num: 0,
        }
    }
}

#[async_trait]
impl FlatMapFunction for RoundRobinFlagMapFunction {
    async fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        self.child_job_parallelism = context.children.len() as u16;
        Ok(())
    }

    async fn flat_map_element(&mut self, element: Element) -> SendableElementStream {
        let mut record = element.into_record();

        if self.partition_num == self.child_job_parallelism {
            self.partition_num = 0;
        }

        record.partition_num = self.partition_num;
        self.partition_num += 1;

        Box::pin(MemoryStream::new(vec![record]))
        // Box::new(vec![record].into_iter())
    }

    async fn close(&mut self) -> crate::core::Result<()> {
        Ok(())
    }

    fn schema(&self, input_schema: FnSchema) -> FnSchema {
        input_schema
    }
}

impl NamedFunction for RoundRobinFlagMapFunction {
    fn name(&self) -> &str {
        "RoundRobinFlagMapFunction"
    }
}

#[async_trait]
impl CheckpointFunction for RoundRobinFlagMapFunction {
    async fn initialize_state(
        &mut self,
        _context: &FunctionSnapshotContext,
        _handle: &Option<CheckpointHandle>,
    ) {
    }

    async fn snapshot_state(
        &mut self,
        _context: &FunctionSnapshotContext,
    ) -> Option<CheckpointHandle> {
        None
    }
}
