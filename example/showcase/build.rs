use serbuffer_gen::{Codegen, DataType::*};

fn main() {
    Codegen::new("src/buffer_gen", "Model")
        .field("timestamp", U64)
        .field("name", STRING)
        .field("value", I64)
        .gen()
        .expect("buffer gen error");

    Codegen::new("src/buffer_gen", "Config")
        .field("field", STRING)
        .field("value", STRING)
        .gen()
        .expect("buffer gen error");
}
