use crate::api::element::{Element, Partition, Record};
use crate::api::function::{Context, Function};
use crate::api::output::OutputFormat;
use crate::channel::{ElementReceiver, ElementSender};
use crate::runtime::worker::io::create_net_channel;
use std::time::Duration;

#[derive(Debug)]
pub(crate) struct NetChannelOutputFormat {
    next_chain_parallelism: u32,
    senders: Vec<ElementSender>,
}

impl NetChannelOutputFormat {
    pub fn new(chain_id: u32, next_chain_parallelism: u32) -> Self {
        let c: Vec<(ElementSender, ElementReceiver)> =
            create_net_channel(chain_id, next_chain_parallelism);

        let senders = c.iter().map(|(tx, _rx)| tx.clone()).collect();

        NetChannelOutputFormat {
            next_chain_parallelism,
            senders,
        }
    }
}

impl OutputFormat for NetChannelOutputFormat {
    fn open(&mut self, context: &Context) {
        info!(
            "OutputFormat ({}) open, task_number={}, num_tasks={}",
            self.get_name(),
            context.task_number,
            context.num_tasks
        );
    }

    fn write_record(&mut self, _record: Record) {}

    fn write_element(&mut self, element: Element) {
        // if element.is_watermark() {
        //     info!("channel send watermark");
        // }

        let sender = self.senders.get(element.get_partition() as usize).unwrap();
        sender.try_send_loop(element, Duration::from_secs(1));

        // todo delete debug code
        // if element.is_record() {
        //     let key = element.as_record().get_string(0).unwrap();
        //     info!(
        //         "Sender Key:{} to Partition {}",
        //         key,
        //         element.get_partition()
        //     );
        // }
    }

    fn close(&mut self) {}
}

impl Function for NetChannelOutputFormat {
    fn get_name(&self) -> &str {
        "NetChannelOutputFormat"
    }
}
