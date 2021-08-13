use serbuffer_gen::{Codegen, DataType::*};

fn main() {
    Codegen::new("src/buffer_gen", "KafkaMessage")
        .field("timestamp", I64)
        .field("key", BINARY)
        .field("payload", BINARY)
        .field("topic", STRING)
        .field("partition", I32)
        .field("offset", I64)
        .gen()
        .expect("buffer gen error");
}
