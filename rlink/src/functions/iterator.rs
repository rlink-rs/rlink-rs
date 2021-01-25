use crate::channel::receiver::ChannelReceiver;
use crate::channel::select::ChannelSelect;
use crate::channel::TryRecvError;

pub struct ChannelIterator<T>
where
    T: Clone,
{
    receiver: ChannelReceiver<T>,
}

impl<T> ChannelIterator<T>
where
    T: Clone,
{
    pub fn new(receiver: ChannelReceiver<T>) -> Self {
        ChannelIterator { receiver }
    }
}

impl<T> Iterator for ChannelIterator<T>
where
    T: Clone,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.receiver.recv() {
            Ok(t) => Some(t),
            Err(_e) => panic!("the channel is Disconnected"),
        }
    }
}

pub struct MultiChannelIterator<T>
where
    T: Clone,
{
    receivers: Vec<ChannelReceiver<T>>,
}

impl<T> MultiChannelIterator<T>
where
    T: Clone,
{
    pub fn new(receivers: Vec<ChannelReceiver<T>>) -> Self {
        MultiChannelIterator { receivers }
    }
}

impl<T> Iterator for MultiChannelIterator<T>
where
    T: Clone,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        // Build a list of operations.
        let mut sel = ChannelSelect::new();
        for r in &self.receivers {
            sel.recv(r);
        }

        loop {
            // Wait until a receive operation becomes ready and try executing it.
            let index = sel.ready();
            let res = self.receivers[index].try_recv();

            match res {
                Ok(t) => {
                    return Some(t);
                }
                Err(TryRecvError::Empty) => continue,
                Err(TryRecvError::Disconnected) => panic!("the channel is Disconnected"),
            }
        }
    }
}
