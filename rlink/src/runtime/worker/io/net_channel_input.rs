use tokio::time::Duration;

use crate::api::element::{Element, Record};
use crate::api::function::{
    Context, Function, InputFormat, InputSplit, InputSplitAssigner, InputSplitSource,
};
use crate::api::properties::Properties;
use crate::channel::{mb, named_bounded, ElementReceiver, TryRecvError};
use crate::metrics::Tag;
use crate::runtime::worker::io::NET_IN_CHANNEL_SIZE;
use crate::storage::metadata::MetadataLoader;
use crate::utils;

#[derive(Debug)]
pub(crate) struct NetChannelInputFormat {
    // task_descriptor: TaskDescriptor,
    chain_id: u32,
    dependency_chain_id: u32,
    metadata_loader: MetadataLoader,
    receiver: Option<ElementReceiver>,
    worker_client_pool: Option<WorkerClientPool>,
}

impl NetChannelInputFormat {
    pub fn new(chain_id: u32, dependency_chain_id: u32, metadata_loader: MetadataLoader) -> Self {
        NetChannelInputFormat {
            // task_descriptor,
            chain_id,
            dependency_chain_id,
            metadata_loader,
            receiver: None,
            worker_client_pool: None,
        }
    }
}

impl InputSplitSource for NetChannelInputFormat {
    fn create_input_splits(&self, min_num_splits: u32) -> Vec<InputSplit> {
        let mut input_splits = Vec::with_capacity(min_num_splits as usize);
        for partition_num in 0..min_num_splits {
            input_splits.push(InputSplit::new(partition_num, Properties::new()));
        }
        input_splits
    }

    fn get_input_split_assigner(&self, _input_splits: Vec<InputSplit>) -> InputSplitAssigner {
        unimplemented!()
    }
}

impl Function for NetChannelInputFormat {
    fn get_name(&self) -> &str {
        "NetChannelInputFormat"
    }
}

impl InputFormat for NetChannelInputFormat {
    fn open(&mut self, input_split: InputSplit, context: &Context) {
        // the partition number. Usually the total of partitions equals the reduce parallelism.
        let partition_number = input_split.get_split_number() as u16;

        let tags = vec![
            Tag("chain_id".to_string(), format!("{}", self.chain_id)),
            Tag("task_number".to_string(), format!("{}", partition_number)),
        ];

        let (tx, rx) = named_bounded::<Element>("Net_Input", tags, NET_IN_CHANNEL_SIZE, mb(50));
        let worker_client_pool = WorkerClientPool::new(
            partition_number,
            self.metadata_loader.clone(),
            self.chain_id,
            context.task_number,
            self.dependency_chain_id,
            tx,
        );

        let mut worker_client_pool_clone = worker_client_pool.clone();
        utils::spawn("net-channel-input-format", move || {
            worker_client_pool_clone.build();
        });

        self.receiver = Some(rx);
        self.worker_client_pool = Some(worker_client_pool);

        info!(
            "PollInputFormat({}) open. partition_num: {}",
            self.get_name(),
            input_split.get_split_number()
        );
    }

    fn reached_end(&self) -> bool {
        false
    }

    fn next_record(&mut self) -> Option<Record> {
        None
    }

    fn next_element(&mut self) -> Option<Element> {
        match self.receiver.as_ref().unwrap().try_recv() {
            Ok(element) => {
                // todo delete debug log
                if element.is_watermark() {
                    debug!(
                        "recv watermark {}",
                        utils::date_time::fmt_date_time(
                            Duration::from_millis(element.as_watermark().timestamp),
                            utils::date_time::FMT_DATE_TIME_1
                        )
                    );
                }
                Some(element)
            }
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                panic!("{} receiver `Disconnected`", self.get_name());
            }
        }
    }

    fn close(&mut self) {}
}
