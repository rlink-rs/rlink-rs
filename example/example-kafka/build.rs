use serbuffer_gen::{Codegen, DataType::*};

fn main() {
    Codegen::new("src/buffer_gen","checkpoint_data")
    .field("ts",U64)
    .field("app",STRING)
    .field("count",I64)
    .gen()
    .expect("buffer gen error");
    
}