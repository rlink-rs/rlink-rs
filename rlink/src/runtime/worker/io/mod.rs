use std::collections::HashMap;
use std::sync::Mutex;

use crate::channel::{mb, named_bounded, ElementReceiver, ElementSender};
use crate::metrics::Tag;

pub(crate) mod mem_channel_input;
pub(crate) mod mem_channel_output;
pub(crate) mod net_channel_input;
pub(crate) mod net_channel_output;

pub const NET_IN_CHANNEL_SIZE: usize = 50000;
pub const NET_OUT_CHANNEL_SIZE: usize = 50000;

lazy_static! {
    // HashMap<u32(chain_id), Vec<(Sender, Receiver)>>
    static ref NET_CHANNEL: Mutex<HashMap<u32, Vec<(ElementSender, ElementReceiver)>>> = Mutex::new(HashMap::new());
    // HashMap<u32(chain_id-task_id), (Sender, Receiver)>
    static ref MEM_CHANNEL: Mutex<HashMap<String, (ElementSender, ElementReceiver)>> = Mutex::new(HashMap::new());
}

pub fn get_net_receivers() -> HashMap<u32, Vec<ElementReceiver>> {
    let lock = NET_CHANNEL.lock().expect("lock failed");
    let chain_channels = (&*lock).clone();

    let mut chain_receivers = HashMap::new();
    for (chain_id, channels) in chain_channels {
        let receivers = channels.iter().map(|(_tx, rx)| rx.clone()).collect();
        chain_receivers.insert(chain_id, receivers);
    }

    chain_receivers
}

pub fn create_net_channel(
    chain_id: u32,
    follower_chain_parallelism: u32,
) -> Vec<(ElementSender, ElementReceiver)> {
    let mut lock = NET_CHANNEL.lock().expect("lock failed");

    (&mut *lock).entry(chain_id).or_insert_with(|| {
        let mut c = Vec::with_capacity(follower_chain_parallelism as usize);
        for partition_num in 0..follower_chain_parallelism {
            let tags = vec![
                Tag("chain_id".to_string(), format!("{}", chain_id)),
                Tag(
                    "follower_partition_num".to_string(),
                    format!("{}", partition_num),
                ),
            ];

            let (tx, rx) = named_bounded("Net_Output", tags, NET_OUT_CHANNEL_SIZE, mb(50));
            c.push((tx, rx));
        }
        c
    });

    (&*lock).get(&chain_id).unwrap().clone()
}

pub fn create_mem_channel(chain_id: u32, task_number: u16) -> (ElementSender, ElementReceiver) {
    info!(
        "Create memory channel `chain_id`={}, `task_number`={}",
        chain_id, task_number
    );

    let key = format!("{}-{}", chain_id, task_number);

    let mut lock = MEM_CHANNEL.lock().expect("lock failed");

    (&mut *lock).entry(key.clone()).or_insert_with(|| {
        let tags = vec![
            Tag("chain_id".to_string(), format!("{}", chain_id)),
            Tag("task_number".to_string(), format!("{}", task_number)),
        ];

        named_bounded("Memory", tags, 1024, mb(1))
    });

    (&*lock).get(key.as_str()).unwrap().clone()
}
