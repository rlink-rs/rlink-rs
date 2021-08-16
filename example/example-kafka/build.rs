use serbuffer_gen::{Codegen, DataType::*, SchemaBuilder};

fn main() {
    Codegen::out_dir("buffer_gen")
        .schema(
            SchemaBuilder::new("checkpoint_data")
                .field("ts", U64)
                .field("app", STRING)
                .field("count", I64),
        )
        .gen()
        .expect("buffer gen error");
}
