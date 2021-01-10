use std::collections::HashMap;
use std::sync::Mutex;

use crate::api::runtime::TaskId;
use crate::channel::{mb, named_bounded, ElementReceiver, ElementSender};
use crate::io::ChannelKey;
use crate::metrics::Tag;

lazy_static! {
    static ref MEMORY_CHANNELS: Mutex<HashMap<ChannelKey, (ElementSender, ElementReceiver)>> =
        Mutex::new(HashMap::new());
}

pub(crate) fn publish(
    source_task_id: &TaskId,
    target_task_ids: &Vec<TaskId>,
) -> Vec<(ChannelKey, ElementSender)> {
    let mut senders = Vec::new();
    for target_task_id in target_task_ids {
        let channel_key = ChannelKey {
            source_task_id: source_task_id.clone(),
            target_task_id: target_task_id.clone(),
        };
        let sender = get(channel_key.clone()).0;
        senders.push((channel_key, sender));
    }
    senders
}

pub(crate) fn subscribe(source_task_ids: &Vec<TaskId>, target_task_id: &TaskId) -> ElementReceiver {
    if source_task_ids.len() != 1 {
        panic!("");
    }

    let channel_key = ChannelKey {
        source_task_id: source_task_ids[0].clone(),
        target_task_id: target_task_id.clone(),
    };
    get(channel_key).1
}

pub(crate) fn get(key: ChannelKey) -> (ElementSender, ElementReceiver) {
    let memory_channels: &Mutex<HashMap<ChannelKey, (ElementSender, ElementReceiver)>> =
        &*MEMORY_CHANNELS;
    let mut guard = memory_channels.lock().unwrap();
    let (sender, receiver) = guard.entry(key.clone()).or_insert_with(|| {
        let ChannelKey {
            source_task_id,
            target_task_id,
        } = key;
        named_bounded(
            "Memory_PubSub",
            vec![
                Tag::new(
                    "source_job_id".to_string(),
                    source_task_id.job_id.to_string(),
                ),
                Tag::new(
                    "target_job_id".to_string(),
                    target_task_id.job_id.to_string(),
                ),
                Tag::new(
                    "target_task_number".to_string(),
                    target_task_id.task_number.to_string(),
                ),
            ],
            100000,
            mb(10),
        )
    });
    (sender.clone(), receiver.clone())
}
