use crate::core::checkpoint::CheckpointFunction;
use crate::core::element::{Record, Schema};
use crate::core::function::{Context, FlatMapFunction, NamedFunction};

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

impl FlatMapFunction for BroadcastFlagMapFunction {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        // if context.children.len() != 1{
        //     panic!("BroadcastFlagMapFunction must has only one child job");
        // }

        self.child_job_parallelism = context.children.len() as u16;

        Ok(())
    }

    fn flat_map(&mut self, record: Record) -> Box<dyn Iterator<Item = Record>> {
        let mut records = Vec::new();
        for partition_num in 0..self.child_job_parallelism {
            let mut r = record.clone();
            r.partition_num = partition_num;
            records.push(r);
        }

        Box::new(records.into_iter())
    }

    fn close(&mut self) -> crate::core::Result<()> {
        Ok(())
    }

    fn schema(&self, input_schema: Schema) -> Schema {
        input_schema
    }
}

impl NamedFunction for BroadcastFlagMapFunction {
    fn name(&self) -> &str {
        "BroadcastFlagMapFunction"
    }
}

impl CheckpointFunction for BroadcastFlagMapFunction {}
