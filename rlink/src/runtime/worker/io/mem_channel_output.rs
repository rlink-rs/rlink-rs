use crate::api::element::{Element, Record};
use crate::api::function::{Context, Function};
use crate::api::output::OutputFormat;
use crate::channel::ElementSender;
use crate::runtime::worker::io::create_mem_channel;
use std::time::Duration;

#[derive(Debug)]
pub(crate) struct MemChannelOutputFormat {
    chain_id: u32,
    sender: Option<ElementSender>,
}

impl MemChannelOutputFormat {
    pub fn new(chain_id: u32) -> Self {
        MemChannelOutputFormat {
            chain_id,
            sender: None,
        }
    }
}

impl OutputFormat for MemChannelOutputFormat {
    fn open(&mut self, context: &Context) {
        info!("OutputFormat ({}) open", self.get_name());

        self.sender = Some(create_mem_channel(self.chain_id, context.task_number).0);
    }

    fn write_record(&mut self, _record: Record) {}

    fn write_element(&mut self, element: Element) {
        if !element.is_watermark() {
            error!("Only `Watermark` can be handle");
            return;
        }

        self.sender
            .as_ref()
            .unwrap()
            .try_send_loop(element, Duration::from_secs(1));
    }

    fn close(&mut self) {}
}

impl Function for MemChannelOutputFormat {
    fn get_name(&self) -> &str {
        "MemChannelOutputFormat"
    }
}
