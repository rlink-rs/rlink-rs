use serbuffer_gen::{Codegen, DataType::*};

fn main() {
    Codegen::new("src/buffer_gen", "Model")
        .field("timestamp", U64)
        .field("id", U32)
        .field("name", STRING)
        .field("value1", I32)
        .field("value2", U32)
        .field("value3", I64)
        .gen()
        .expect("buffer gen error");
}
