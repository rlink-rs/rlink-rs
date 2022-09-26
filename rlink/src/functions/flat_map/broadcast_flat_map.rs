use crate::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::{Element, FnSchema};
use crate::core::function::{Context, FlatMapFunction, NamedFunction, SendableElementStream};
use crate::utils::stream::MemoryStream;

pub struct BroadcastFlagMapFunction {
    child_job_parallelism: u16,
}

impl BroadcastFlagMapFunction {
    pub fn new() -> Self {
        BroadcastFlagMapFunction {
            child_job_parallelism: 0,
        }
    }
}

#[async_trait]
impl FlatMapFunction for BroadcastFlagMapFunction {
    async fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        // if context.children.len() != 1{
        //     panic!("BroadcastFlagMapFunction must has only one child job");
        // }

        self.child_job_parallelism = context.children.len() as u16;

        Ok(())
    }

    async fn flat_map_element(&mut self, element: Element) -> SendableElementStream {
        let record = element.into_record();
        let mut records = Vec::new();
        for partition_num in 0..self.child_job_parallelism {
            let mut r = record.clone();
            r.partition_num = partition_num;
            records.push(r);
        }

        Box::pin(MemoryStream::new(records))
        // Box::new(records.into_iter())
    }

    async fn close(&mut self) -> crate::core::Result<()> {
        Ok(())
    }

    fn schema(&self, input_schema: FnSchema) -> FnSchema {
        input_schema
    }
}

impl NamedFunction for BroadcastFlagMapFunction {
    fn name(&self) -> &str {
        "BroadcastFlagMapFunction"
    }
}

#[async_trait]
impl CheckpointFunction for BroadcastFlagMapFunction {
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
