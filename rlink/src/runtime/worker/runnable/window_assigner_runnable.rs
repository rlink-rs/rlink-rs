use std::borrow::BorrowMut;

use crate::api::element::Element;
use crate::api::operator::StreamOperator;
use crate::api::runtime::CheckpointId;
use crate::api::window::{WindowAssigner, WindowAssignerContext};
use crate::runtime::worker::runnable::{Runnable, RunnableContext};

#[derive(Debug)]
pub(crate) struct WindowAssignerRunnable {
    stream_window: StreamOperator<dyn WindowAssigner>,
    next_runnable: Option<Box<dyn Runnable>>,
}

impl WindowAssignerRunnable {
    pub fn new(
        stream_window: StreamOperator<dyn WindowAssigner>,
        next_runnable: Option<Box<dyn Runnable>>,
    ) -> Self {
        info!("Create WindowAssignerRunnable");

        WindowAssignerRunnable {
            stream_window,
            next_runnable,
        }
    }
}

impl Runnable for WindowAssignerRunnable {
    fn open(&mut self, context: &RunnableContext) {
        self.next_runnable.as_mut().unwrap().open(context);
        info!(
            "WindowAssignerRunnable({}) opened",
            self.stream_window.operator_fn.get_name()
        );
    }

    fn run(&mut self, mut element: Element) {
        match element.borrow_mut() {
            Element::Record(record) => {
                let windows = self
                    .stream_window
                    .operator_fn
                    .assign_windows(record.timestamp, WindowAssignerContext {});

                // info!(
                //     "Create windows, trigger timestamp: {}",
                //     timestamp_str(record.timestamp)
                // );
                // for window in &windows {
                //     info!("Assign window: {}", window);
                // }

                record.set_location_windows(windows);
            }
            Element::Watermark(watermark) => {
                let windows = self
                    .stream_window
                    .operator_fn
                    .assign_windows(watermark.timestamp, WindowAssignerContext {});

                // info!(
                //     "Operate `Watermark`({})",
                //     timestamp_str(watermark.timestamp)
                // );
                // for window in &windows {
                //     info!(
                //         "Assign window: [{}/{}]",
                //         timestamp_str(window.min_timestamp()),
                //         timestamp_str(window.max_timestamp())
                //     );
                // }

                watermark.set_location_windows(windows);
            }
            _ => {}
        }

        self.next_runnable.as_mut().unwrap().run(element);
    }

    fn close(&mut self) {
        self.next_runnable.as_mut().unwrap().close();
    }

    fn set_next_runnable(&mut self, next_runnable: Option<Box<dyn Runnable>>) {
        self.next_runnable = next_runnable;
    }

    fn checkpoint(&mut self, _checkpoint_id: CheckpointId) {}
}
