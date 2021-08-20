use crate::channel::receiver::ChannelReceiver;

pub struct ChannelIterator<T>
where
    T: Sync + Send,
{
    receiver: ChannelReceiver<T>,
}

impl<T> ChannelIterator<T>
where
    T: Sync + Send,
{
    pub fn new(receiver: ChannelReceiver<T>) -> Self {
        ChannelIterator { receiver }
    }
}

impl<T> Iterator for ChannelIterator<T>
where
    T: Sync + Send,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(t) => Some(t),
            Err(_e) => {
                info!("the channel is Disconnected");
                None
            }
        }
    }
}
