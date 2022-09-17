use std::collections::HashMap;
use std::sync::Mutex;

use crate::channel::{named_channel, ElementReceiver, ElementSender};
use crate::core::runtime::{ChannelKey, TaskId};
use crate::metrics::Tag;

lazy_static! {
    static ref MEMORY_CHANNELS: Mutex<HashMap<TaskId, (ElementSender, Option<ElementReceiver>)>> =
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
        insert(*target_task_id, channel_size);
        let sender = get_sender(target_task_id).unwrap();
        senders.push((channel_key, sender));
    }
    senders
}

pub(crate) fn subscribe(
    source_task_ids: &Vec<TaskId>,
    target_task_id: &TaskId,
    channel_size: usize,
    // channel_base_on: ChannelBaseOn,
) -> ElementReceiver {
    if source_task_ids.len() == 0 {
        panic!("source TaskId not found");
    }

    insert(*target_task_id, channel_size);
    get_receiver(target_task_id).expect("receiver not found, maybe a duplicate subscribe")
}

pub(crate) fn insert(target_task_id: TaskId, channel_size: usize) {
    let memory_channels: &Mutex<HashMap<TaskId, (ElementSender, Option<ElementReceiver>)>> =
        &*MEMORY_CHANNELS;
    let mut guard = memory_channels.lock().unwrap();
    guard.entry(target_task_id).or_insert_with(|| {
        let (sender, receiver) = named_channel(
            "Memory_PubSub",
            vec![
                Tag::new("target_job_id", target_task_id.job_id.0),
                Tag::new("target_task_number", target_task_id.task_number),
            ],
            channel_size,
        );
        (sender, Some(receiver))
    });
}

pub(crate) fn get_receiver(target_task_id: &TaskId) -> Option<ElementReceiver> {
    let memory_channels: &Mutex<HashMap<TaskId, (ElementSender, Option<ElementReceiver>)>> =
        &*MEMORY_CHANNELS;
    let mut guard = memory_channels.lock().unwrap();
    guard
        .get_mut(&target_task_id)
        .map(|(_sender, receiver)| receiver.take())
        .unwrap_or_default()
}

pub(crate) fn get_sender(target_task_id: &TaskId) -> Option<ElementSender> {
    let memory_channels: &Mutex<HashMap<TaskId, (ElementSender, Option<ElementReceiver>)>> =
        &*MEMORY_CHANNELS;
    let mut guard = memory_channels.lock().unwrap();
    guard
        .get_mut(&target_task_id)
        .map(|(sender, _receiver)| sender.clone())
}
