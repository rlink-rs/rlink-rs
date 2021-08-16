use serbuffer_gen::{Codegen, DataType::*, SchemaBuilder};

fn main() {
    Codegen::out_dir("buffer_gen")
        .schema(
            SchemaBuilder::new("KafkaMessage")
                .field("timestamp", I64)
                .field("key", BINARY)
                .field("payload", BINARY)
                .field("topic", STRING)
                .field("partition", I32)
                .field("offset", I64),
        )
        .gen()
        .expect("buffer gen error");
}
