pub mod system_input_format;
pub mod system_output_format;

pub mod memory;
pub mod network;

#[derive(Copy, Clone, Debug)]
pub(crate) enum ChannelType {
    Memory = 1,
    Network = 2,
}
