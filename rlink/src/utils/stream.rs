use std::pin::Pin;
use std::task::Poll;

use futures::Stream;

use crate::core::element::{Element, Record};
use crate::core::function::ElementStream;

/// Iterator over batches
pub struct MemoryStream {
    /// Vector of record batches
    data: Vec<Record>,
    /// Index into the data
    index: usize,
}

impl MemoryStream {
    pub fn new(data: Vec<Record>) -> Self {
        Self { data, index: 0 }
    }
}

impl ElementStream for MemoryStream {}

impl Stream for MemoryStream {
    type Item = Element;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            self.index += 1;
            let batch = &self.data[self.index - 1];
            let element = Element::Record(batch.clone());

            Some(element)
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

/// Iterator over batches
pub struct IteratorStream {
    /// Vector of record batches
    data: Box<dyn Iterator<Item = Record> + Send>,
    /// Index into the data
    index: usize,
}

impl IteratorStream {
    pub fn new(data: Box<dyn Iterator<Item = Record> + Send>) -> Self {
        Self { data, index: 0 }
    }
}

impl ElementStream for IteratorStream {}

impl Stream for IteratorStream {
    type Item = Element;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let e = match self.data.next() {
            Some(record) => {
                self.index += 1;
                Some(Element::Record(record))
            }
            None => None,
        };
        Poll::Ready(e)
    }
}
