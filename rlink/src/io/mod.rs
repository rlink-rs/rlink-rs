use crate::api::runtime::TaskId;

pub mod system_input_format;
pub mod system_keyed_state_flat_map;
pub mod system_output_format;

pub mod memory;
pub mod network;

#[derive(Copy, Clone, Debug)]
pub(crate) enum ChannelType {
    Memory = 1,
    Network = 2,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct ChannelKey {
    pub(crate) source_task_id: TaskId,
    pub(crate) target_task_id: TaskId,
}
