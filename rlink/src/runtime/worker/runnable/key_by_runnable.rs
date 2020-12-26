use crate::api::element::Element;
use crate::api::function::KeySelectorFunction;
use crate::api::operator::StreamOperator;
use crate::metrics::{register_counter, Tag};
use crate::runtime::worker::runnable::{Runnable, RunnableContext};
use crate::utils;
use std::borrow::BorrowMut;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct KeyByRunnable {
    stream_key_by: StreamOperator<dyn KeySelectorFunction>,
    next_runnable: Option<Box<dyn Runnable>>,
    partition_size: u16,

    counter: Arc<AtomicU64>,
}

impl KeyByRunnable {
    pub fn new(
        stream_key_by: StreamOperator<dyn KeySelectorFunction>,
        next_runnable: Option<Box<dyn Runnable>>,
        partition_size: u16,
    ) -> Self {
        info!("Create KeyByRunnable partition_size={}", partition_size);

        KeyByRunnable {
            stream_key_by,
            next_runnable,
            partition_size,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Runnable for KeyByRunnable {
    fn open(&mut self, context: &RunnableContext) {
        self.next_runnable.as_mut().unwrap().open(context);

        let fun_context = context.to_fun_context();
        self.stream_key_by.operator_fn.open(&fun_context);

        // for partition_num in 0..self.partition_size {
        //     self.sink_counter.push(format!(
        //         "{}.{}.KeyBy_{}.Counter",
        //         self.stream_key_by.key_by_fn.get_name(),
        //         context.task_descriptor.task_id.clone(),
        //         partition_num
        //     ));
        // }

        let tags = vec![
            Tag(
                "chain_id".to_string(),
                context.task_descriptor.chain_id.to_string(),
            ),
            Tag(
                "partition_num".to_string(),
                context.task_descriptor.task_number.to_string(),
            ),
        ];
        let metric_name = format!(
            "KeyBy_{}",
            self.stream_key_by.operator_fn.as_ref().get_name()
        );
        register_counter(metric_name.as_str(), tags, self.counter.clone());
    }

    fn run(&mut self, mut element: Element) {
        match element.borrow_mut() {
            Element::Record(record) => {
                let key_row = self
                    .stream_key_by
                    .operator_fn
                    .as_mut()
                    .get_key(record.borrow_mut());

                let hash_code = utils::hash::hash_code(key_row.values.as_slice()).unwrap_or(0);
                let partition_num = hash_code % self.partition_size as u32;
                // info!(
                //     "partition: {}, hash code: {}, partition_size: {}",
                //     partition_num,
                //     hash_code,
                //     self.partition_size,
                // );
                record.partition_num = partition_num as u16;

                self.next_runnable.as_mut().unwrap().run(element);

                self.counter.fetch_add(1, Ordering::Relaxed);
            }
            Element::Watermark(watermark) => {
                for index in 0..self.partition_size {
                    let mut row_watermark = watermark.clone();
                    row_watermark.partition_num = index as u16;

                    self.next_runnable
                        .as_mut()
                        .unwrap()
                        .run(Element::Watermark(row_watermark));
                }
            }
            Element::Barrier(barrier) => {
                for index in 0..self.partition_size {
                    let mut row_barrier = barrier.clone();
                    row_barrier.partition_num = index as u16;

                    self.next_runnable
                        .as_mut()
                        .unwrap()
                        .run(Element::Barrier(row_barrier));
                }
            }
            _ => {}
        }
    }

    fn close(&mut self) {
        self.stream_key_by.operator_fn.close();
        self.next_runnable.as_mut().unwrap().close();
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, _checkpoint_id: u64) {}
}
