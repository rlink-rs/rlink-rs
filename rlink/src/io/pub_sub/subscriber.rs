use dashmap::DashMap;

use crate::channel::ElementSender;
use crate::io::network::client;
use crate::io::pub_sub::ChannelKey;

lazy_static! {
    static ref MEMORY_CHANNELS: DashMap<ChannelKey, ElementSender> = DashMap::new();
    static ref NETWORK_CHANNELS: DashMap<ChannelKey, ElementSender> = DashMap::new();
}

pub(crate) fn set_memory_channel(key: ChannelKey, sender: ElementSender) {
    let memory_channels: &DashMap<ChannelKey, ElementSender> = &*MEMORY_CHANNELS;
    memory_channels.insert(key, sender);
}

pub(crate) fn get_memory_channel(key: &ChannelKey) -> Option<ElementSender> {
    let memory_channels: &DashMap<ChannelKey, ElementSender> = &*MEMORY_CHANNELS;
    memory_channels.get(key).map(|x| x.value().clone())
}

pub(crate) fn set_network_channel(key: ChannelKey, sender: ElementSender) {
    let network_channels: &DashMap<ChannelKey, ElementSender> = &*NETWORK_CHANNELS;
    network_channels.insert(key.clone(), sender.clone());
    client::subscribe_post(key, sender);
}

// pub(crate) fn get_network_channel(key: &ChannelKey) -> Option<ElementSender> {
//     let network_channels: &DashMap<ChannelKey, ElementSender> = &*NETWORK_CHANNELS;
//     network_channels.get(key).map(|x| x.value().clone())
// }
