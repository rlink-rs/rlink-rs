use crate::api::runtime::TaskId;
use crate::channel::{mb, named_bounded, ElementReceiver, ElementSender};
use crate::metrics::Tag;

pub mod publisher;
pub mod subscriber;

#[derive(Copy, Clone, Debug)]
pub(crate) enum ChannelType {
    Memory = 1,
    Network = 2,
}

impl std::fmt::Display for ChannelType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelType::Memory => write!(f, "Memory"),
            ChannelType::Network => write!(f, "Network"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ChannelKey {
    pub(crate) source_task_id: TaskId,
    pub(crate) target_task_id: TaskId,
}

pub(crate) fn publish(
    source_task_id: TaskId,
    target_task_ids: Vec<TaskId>,
    channel_type: ChannelType,
) -> Vec<ElementSender> {
    let mut senders = Vec::new();
    for target_task_id in target_task_ids {
        let key = ChannelKey {
            source_task_id: source_task_id.clone(),
            target_task_id: target_task_id.clone(),
        };

        let (sender, receiver) = named_bounded(
            format!("Publish_{}", channel_type).as_str(),
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
        );

        senders.push(sender);

        match channel_type {
            ChannelType::Memory => publisher::set_memory_channel(key, receiver),
            ChannelType::Network => publisher::set_network_channel(key, receiver),
        }
    }

    senders
}

pub(crate) struct SubKey {
    source_task_id: TaskId,
    target_task_id: TaskId,
}

pub(crate) fn subscribe(
    source_task_ids: Vec<TaskId>,
    target_task_id: TaskId,
    channel_type: ChannelType,
) -> ElementReceiver {
    let (sender, receiver) = named_bounded(
        format!("Subscribe_{}", channel_type).as_str(),
        vec![
            Tag::new(
                "source_job_id".to_string(),
                source_task_ids[0].job_id.to_string(),
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
    );

    for source_task_id in source_task_ids {
        let sub_key = ChannelKey {
            source_task_id,
            target_task_id: target_task_id.clone(),
        };

        match channel_type {
            ChannelType::Memory => subscriber::set_memory_channel(sub_key, sender.clone()),
            ChannelType::Network => subscriber::set_network_channel(sub_key, sender.clone()),
        }
    }

    receiver
}
