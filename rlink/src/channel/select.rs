use crate::channel::receiver::ChannelReceiver;
use crate::channel::Select;

pub struct ChannelSelect<'a> {
    select: Select<'a>,
}

impl<'a> ChannelSelect<'a> {
    pub fn new() -> Self {
        ChannelSelect {
            select: Select::new(),
        }
    }

    pub fn recv<T>(&mut self, r: &'a ChannelReceiver<T>) -> usize
    where
        T: Clone,
    {
        self.select.recv(&r.receiver)
    }

    pub fn ready(&mut self) -> usize {
        self.select.ready()
    }
}
