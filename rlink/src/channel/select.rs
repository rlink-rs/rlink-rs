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

// #[cfg(test)]
// mod tests {
//     use crossbeam::channel::Receiver;
//     use crossbeam::channel::Select;
//     use crossbeam::channel::TryRecvError;
//
//     struct MultiChannelIterator<'a> {
//         receivers: Vec<Receiver<u32>>,
//         select: Select<'a>,
//         sel: *const Select<'a>,
//     }
//
//     impl<'a> MultiChannelIterator<'a> {
//         pub fn new(receivers: Vec<Receiver<u32>>) -> Self {
//             // Build a list of operations.
//             let mut select = Select::new();
//             for r in &receivers {
//                 select.recv(r);
//             }
//
//             let n = &select as *const _;
//
//             MultiChannelIterator { receivers, select }
//         }
//     }
//
//     impl<'a> Iterator for MultiChannelIterator<'a> {
//         type Item = u32;
//
//         fn next(&mut self) -> Option<Self::Item> {
//             // Build a list of operations.
//             let mut sel = Select::new();
//             for r in &self.receivers {
//                 sel.recv(r);
//             }
//
//             loop {
//                 // Wait until a receive operation becomes ready and try executing it.
//                 let index = sel.ready();
//                 let res = self.receivers[index].try_recv();
//
//                 match res {
//                     Ok(element) => {
//                         return Some(element);
//                     }
//                     Err(TryRecvError::Empty) => continue,
//                     Err(TryRecvError::Disconnected) => panic!("the channel is Disconnected"),
//                 }
//             }
//         }
//     }
//
//     #[test]
//     pub fn self_ref_test() {}
// }
