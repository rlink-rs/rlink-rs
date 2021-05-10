use std::collections::HashMap;
use std::sync::Mutex;

use crate::channel::{named_channel_with_base, ElementReceiver, ElementSender};
use crate::core::properties::ChannelBaseOn;
use crate::core::runtime::{ChannelKey, TaskId};
use crate::metrics::Tag;

lazy_static! {
    static ref MEMORY_CHANNELS: Mutex<HashMap<TaskId, (ElementSender, ElementReceiver)>> =
        Mutex::new(HashMap::new());
}

pub(crate) fn publish(
    source_task_id: &TaskId,
    target_task_ids: &Vec<TaskId>,
    channel_size: usize,
    channel_base_on: ChannelBaseOn,
) -> Vec<(ChannelKey, ElementSender)> {
    let mut senders = Vec::new();
    for target_task_id in target_task_ids {
        let channel_key = ChannelKey {
            source_task_id: source_task_id.clone(),
            target_task_id: target_task_id.clone(),
        };
        let sender = get(*target_task_id, channel_size, channel_base_on).0;
        senders.push((channel_key, sender));
    }
    senders
}

pub(crate) fn subscribe(
    source_task_ids: &Vec<TaskId>,
    target_task_id: &TaskId,
    channel_size: usize,
    channel_base_on: ChannelBaseOn,
) -> ElementReceiver {
    if source_task_ids.len() == 0 {
        panic!("source TaskId not found");
    }

    get(*target_task_id, channel_size, channel_base_on).1
}

pub(crate) fn get(
    target_task_id: TaskId,
    channel_size: usize,
    channel_base_on: ChannelBaseOn,
) -> (ElementSender, ElementReceiver) {
    let memory_channels: &Mutex<HashMap<TaskId, (ElementSender, ElementReceiver)>> =
        &*MEMORY_CHANNELS;
    let mut guard = memory_channels.lock().unwrap();
    let (sender, receiver) = guard.entry(target_task_id).or_insert_with(|| {
        named_channel_with_base(
            "Memory_PubSub",
            vec![
                Tag::new("target_job_id", target_task_id.job_id.0),
                Tag::new("target_task_number", target_task_id.task_number),
            ],
            channel_size,
            channel_base_on,
        )
    });
    (sender.clone(), receiver.clone())
}
