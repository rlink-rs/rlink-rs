use crate::channel::{mb, named_bounded, ElementReceiver, ElementSender};
use crate::dag::TaskId;
use crate::metrics::Tag;
use std::collections::HashMap;

pub mod memory_channel;
pub mod network_channel;

#[derive(Copy, Clone, Debug)]
pub(crate) enum ChannelType {
    Memory = 1,
    Network = 2,
}

/// SourceTaskId

pub(crate) fn publish(
    source_task_id: TaskId,
    target_task_ids: Vec<TaskId>,
    channel_type: ChannelType,
) -> Vec<ElementSender> {
    vec![]
}

pub(crate) fn subscribe(
    source_task_ids: Vec<TaskId>,
    target_task_id: TaskId,
    channel_type: ChannelType,
) -> ElementReceiver {
    let (tx, rx) = named_bounded(
        "subscribe",
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

    match channel_type {
        ChannelType::Memory => {}
        ChannelType::Network => {
            let mut task_tx_map = HashMap::new();
            for source_task_id in source_task_ids {
                task_tx_map.insert(source_task_id, tx.clone());
            }
        }
    }

    rx
}
