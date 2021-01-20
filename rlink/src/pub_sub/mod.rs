pub mod memory;
pub mod network;

#[derive(Copy, Clone, Debug)]
pub(crate) enum ChannelType {
    Memory = 1,
    Network = 2,
}
