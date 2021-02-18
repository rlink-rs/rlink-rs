use crate::api::checkpoint::CheckpointFunction;
use crate::api::element::Record;
use crate::api::function::{Context, FlatMapFunction, NamedFunction};

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

impl FlatMapFunction for RoundRobinFlagMapFunction {
    fn open(&mut self, context: &Context) -> crate::api::Result<()> {
        self.child_job_parallelism = context.children.len() as u16;
        Ok(())
    }

    fn flat_map(&mut self, mut record: Record) -> Box<dyn Iterator<Item = Record>> {
        if self.partition_num == self.child_job_parallelism {
            self.partition_num = 0;
        }

        record.partition_num = self.partition_num;
        self.partition_num += 1;

        Box::new(vec![record].into_iter())
    }

    fn close(&mut self) -> crate::api::Result<()> {
        Ok(())
    }
}

impl NamedFunction for RoundRobinFlagMapFunction {
    fn name(&self) -> &str {
        "RoundRobinFlagMapFunction"
    }
}

impl CheckpointFunction for RoundRobinFlagMapFunction {}
