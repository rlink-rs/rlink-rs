use std::collections::HashMap;

use crate::api::checkpoint::FunctionSnapshotContext;
use crate::api::element::Element;
use crate::api::function::CoProcessFunction;
use crate::api::operator::DefaultStreamOperator;
use crate::api::runtime::{JobId, OperatorId};
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

#[derive(Debug)]
pub(crate) struct CoProcessRunnable {
    operator_id: OperatorId,
    stream_co_process: DefaultStreamOperator<dyn CoProcessFunction>,
    next_runnable: Option<Box<dyn Runnable>>,

    /// key: JobId,
    /// value: DataStream index  
    parent_jobs: HashMap<JobId, usize>,
}

impl CoProcessRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_co_process: DefaultStreamOperator<dyn CoProcessFunction>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create FilterRunnable");

        CoProcessRunnable {
            operator_id,
            stream_co_process,
            next_runnable,
            parent_jobs: HashMap::new(),
        }
    }
}

impl Runnable for CoProcessRunnable {
    fn open(&mut self, context: &RunnableContext) -> anyhow::Result<()> {
        self.next_runnable.as_mut().unwrap().open(context)?;

        // find the stream_node of `input_format`
        // the chain: input_format -> connect, so the `connect` is only one parent
        let source_stream_node = &context.get_job_node().stream_nodes[0];

        for index in 0..source_stream_node.parent_ids.len() {
            let parent_id = source_stream_node.parent_ids[index];
            let parent_job_id = context
                .get_parent_jobs()
                .iter()
                .find_map(|(node, _)| {
                    node.stream_nodes
                        .iter()
                        .find(|x| x.id == parent_id)
                        .map(|_x| node.job_id)
                })
                .ok_or(anyhow!("co_process_function parent not found"))?;
            self.parent_jobs.insert(parent_job_id, index);
        }

        let fun_context = context.to_fun_context(self.operator_id);
        self.stream_co_process.operator_fn.open(&fun_context)?;

        Ok(())
    }

    fn run(&mut self, element: Element) {
        match element {
            Element::Record(record) => {
                let stream_seq = *self
                    .parent_jobs
                    .get(&record.channel_key.source_task_id.job_id)
                    .expect("parent job not found");

                let records = if stream_seq == self.parent_jobs.len() - 1 {
                    self.stream_co_process
                        .operator_fn
                        .as_mut()
                        .process_left(record)
                } else {
                    self.stream_co_process
                        .operator_fn
                        .as_mut()
                        .process_right(stream_seq, record)
                };

                for record in records {
                    self.next_runnable
                        .as_mut()
                        .unwrap()
                        .run(Element::Record(record));
                }
            }
            _ => {
                self.next_runnable.as_mut().unwrap().run(element);
            }
        }
    }

    fn close(&mut self) -> anyhow::Result<()> {
        self.stream_co_process.operator_fn.close()?;
        self.next_runnable.as_mut().unwrap().close()
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, _snapshot_context: FunctionSnapshotContext) {}
}
