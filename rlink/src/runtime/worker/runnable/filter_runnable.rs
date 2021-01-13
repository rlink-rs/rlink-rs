use crate::api::element::Element;
use crate::api::function::FilterFunction;
use crate::api::operator::StreamOperator;
use crate::api::runtime::CheckpointId;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

#[derive(Debug)]
pub(crate) struct FilterRunnable {
    stream_filter: StreamOperator<dyn FilterFunction>,
    next_runnable: Option<Box<dyn Runnable>>,
}

impl FilterRunnable {
    pub fn new(
        stream_filter: StreamOperator<dyn FilterFunction>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create FilterRunnable");

        FilterRunnable {
            stream_filter,
            next_runnable,
        }
    }
}

impl Runnable for FilterRunnable {
    fn open(&mut self, context: &RunnableContext) {
        self.next_runnable.as_mut().unwrap().open(context);

        let fun_context = context.to_fun_context();
        self.stream_filter.operator_fn.open(&fun_context);
    }

    fn run(&mut self, mut element: Element) {
        if element.is_record() {
            if self
                .stream_filter
                .operator_fn
                .as_mut()
                .filter(element.as_record_mut())
            {
                self.next_runnable.as_mut().unwrap().run(element);
            }
        } else {
            self.next_runnable.as_mut().unwrap().run(element);
        }
    }

    fn close(&mut self) {
        self.stream_filter.operator_fn.close();
        self.next_runnable.as_mut().unwrap().close();
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, _checkpoint_id: CheckpointId) {}
}
