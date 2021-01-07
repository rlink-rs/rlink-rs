use dashmap::DashMap;

use crate::channel::ElementSender;
use crate::dag::TaskId;

lazy_static! {
    static ref MEMORY_CHANNELS: DashMap<TaskId, ElementSender> = DashMap::new();
}

pub(crate) struct MemoryChannel {}
