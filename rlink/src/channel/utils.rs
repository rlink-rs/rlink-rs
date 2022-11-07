use std::pin::Pin;
use std::task::Poll;

use futures::Stream;

use crate::channel::receiver::ChannelReceiver;
use crate::core::element::Element;
use crate::core::function::ElementStream;

pub struct ChannelStream {
    receiver: ChannelReceiver<Element>,
}

impl ChannelStream {
    pub fn new(receiver: ChannelReceiver<Element>) -> Self {
        Self { receiver }
    }
}

impl ElementStream for ChannelStream {}

impl Stream for ChannelStream {
    type Item = Element;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.get_mut().receiver.poll_recv(cx)
    }
}
