use std::collections::HashMap;
use std::sync::Mutex;

use crate::api::runtime::{ChannelKey, TaskId};
use crate::channel::{mb, named_bounded, ElementReceiver, ElementSender};
use crate::metrics::Tag;

lazy_static! {
    static ref MEMORY_CHANNELS: Mutex<HashMap<TaskId, (ElementSender, ElementReceiver)>> =
        Mutex::new(HashMap::new());
}

pub(crate) fn publish(
    source_task_id: &TaskId,
    target_task_ids: &Vec<TaskId>,
    channel_size: usize,
) -> Vec<(ChannelKey, ElementSender)> {
    let mut senders = Vec::new();
    for target_task_id in target_task_ids {
        let channel_key = ChannelKey {
            source_task_id: source_task_id.clone(),
            target_task_id: target_task_id.clone(),
        };
        let sender = get(*target_task_id, channel_size).0;
        senders.push((channel_key, sender));
    }
    senders
}

pub(crate) fn subscribe(
    source_task_ids: &Vec<TaskId>,
    target_task_id: &TaskId,
    channel_size: usize,
) -> ElementReceiver {
    if source_task_ids.len() == 0 {
        panic!("source TaskId not found");
    }

    get(*target_task_id, channel_size).1
}

pub(crate) fn get(target_task_id: TaskId, channel_size: usize) -> (ElementSender, ElementReceiver) {
    let memory_channels: &Mutex<HashMap<TaskId, (ElementSender, ElementReceiver)>> =
        &*MEMORY_CHANNELS;
    let mut guard = memory_channels.lock().unwrap();
    let (sender, receiver) = guard.entry(target_task_id).or_insert_with(|| {
        named_bounded(
            "Memory_PubSub",
            vec![
                Tag::new(
                    "target_job_id".to_string(),
                    target_task_id.job_id.0.to_string(),
                ),
                Tag::new(
                    "target_task_number".to_string(),
                    target_task_id.task_number.to_string(),
                ),
            ],
            channel_size,
            mb(10),
        )
    });
    (sender.clone(), receiver.clone())
}
