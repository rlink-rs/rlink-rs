#[macro_use]
extern crate rlink_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

extern crate anyhow;

mod app;
mod buffer_gen;
mod entry;
mod filter;
mod input_mapper;
mod kafka_input_mapper;
mod output_mapper;
mod writer;

fn main() {
    // rlink::core::env::execute(app::KafkaGenAppStream::new());
    // rlink::core::env::execute(app::KafkaOffsetRangeAppStream::new());
    rlink::core::env::execute(app::KafkaReplayAppStream::new());
}
