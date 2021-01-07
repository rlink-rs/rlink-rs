use dashmap::DashMap;

use crate::channel::ElementReceiver;
use crate::io::pub_sub::ChannelKey;

lazy_static! {
    static ref MEMORY_CHANNELS: DashMap<ChannelKey, ElementReceiver> = DashMap::new();
    static ref NETWORK_CHANNELS: DashMap<ChannelKey, ElementReceiver> = DashMap::new();
}

pub(crate) fn set_memory_channel(key: ChannelKey, sender: ElementReceiver) {
    let memory_channels: &DashMap<ChannelKey, ElementReceiver> = &*MEMORY_CHANNELS;

    memory_channels.insert(key, sender);
}

pub(crate) fn get_memory_channel(key: ChannelKey) -> Option<ElementReceiver> {
    let memory_channels: &DashMap<ChannelKey, ElementReceiver> = &*MEMORY_CHANNELS;
    memory_channels.get(&key).map(|x| x.value().clone())
}

pub(crate) fn set_network_channel(key: ChannelKey, sender: ElementReceiver) {
    let network_channels: &DashMap<ChannelKey, ElementReceiver> = &*NETWORK_CHANNELS;

    network_channels.insert(key, sender);
}

pub(crate) fn get_network_channel(key: &ChannelKey) -> Option<ElementReceiver> {
    let network_channels: &DashMap<ChannelKey, ElementReceiver> = &*NETWORK_CHANNELS;
    network_channels.get(key).map(|x| x.value().clone())
}
