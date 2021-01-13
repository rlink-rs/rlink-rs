use crate::api::element::Element;
use crate::api::function::CoProcessFunction;
use crate::api::operator::StreamOperator;
use crate::api::runtime::{CheckpointId, JobId, OperatorId};
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct CoProcessRunnable {
    operator_id: OperatorId,
    stream_co_process: StreamOperator<dyn CoProcessFunction>,
    next_runnable: Option<Box<dyn Runnable>>,

    /// key: JobId,
    /// value: DataStream index  
    parent_jobs: HashMap<JobId, usize>,
}

impl CoProcessRunnable {
    pub fn new(
        operator_id: OperatorId,
        stream_co_process: StreamOperator<dyn CoProcessFunction>,
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
    fn open(&mut self, context: &RunnableContext) {
        self.next_runnable.as_mut().unwrap().open(context);

        let source_stream_node = {
            let stream_node = context.get_stream(self.operator_id);
            context.get_stream(stream_node.parent_ids[0])
        };
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
                .unwrap();
            self.parent_jobs.insert(parent_job_id, index);
        }

        let fun_context = context.to_fun_context();
        self.stream_co_process.operator_fn.open(&fun_context);
    }

    fn run(&mut self, element: Element) {
        match element {
            Element::Record(record) => {
                let stream_seq = *self
                    .parent_jobs
                    .get(&record.channel_key.source_task_id.job_id)
                    .expect("parent job not found");

                let record = if stream_seq == self.parent_jobs.len() - 1 {
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
                match record {
                    Some(record) => {
                        self.next_runnable
                            .as_mut()
                            .unwrap()
                            .run(Element::Record(record));
                    }
                    None => {}
                }
            }
            _ => {
                self.next_runnable.as_mut().unwrap().run(element);
            }
        }
    }

    fn close(&mut self) {
        self.stream_co_process.operator_fn.close();
        self.next_runnable.as_mut().unwrap().close();
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, _checkpoint_id: CheckpointId) {}
}
