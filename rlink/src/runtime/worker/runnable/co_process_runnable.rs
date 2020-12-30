use crate::api::element::Element;
use crate::api::function::CoProcessFunction;
use crate::api::operator::StreamOperator;
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

#[derive(Debug)]
pub(crate) struct CoProcessRunnable {
    stream_co_process: StreamOperator<dyn CoProcessFunction>,
    next_runnable: Option<Box<dyn Runnable>>,
}

impl CoProcessRunnable {
    pub fn new(
        stream_co_process: StreamOperator<dyn CoProcessFunction>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create FilterRunnable");

        CoProcessRunnable {
            stream_co_process,
            next_runnable,
        }
    }
}

impl Runnable for CoProcessRunnable {
    fn open(&mut self, context: &RunnableContext) {
        self.next_runnable.as_mut().unwrap().open(context);

        let fun_context = context.to_fun_context();
        self.stream_co_process.operator_fn.open(&fun_context);
    }

    fn run(&mut self, element: Element) {
        if element.is_record() {
            let r = self
                .stream_co_process
                .operator_fn
                .as_mut()
                .process(0, element.into_record());

            self.next_runnable.as_mut().unwrap().run(Element::Record(r));
        } else {
            self.next_runnable.as_mut().unwrap().run(element);
        }
    }

    fn close(&mut self) {
        self.stream_co_process.operator_fn.close();
        self.next_runnable.as_mut().unwrap().close();
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, _checkpoint_id: u64) {}
}
