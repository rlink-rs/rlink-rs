use serbuffer_gen::{Codegen, DataType::*, SchemaBuilder};

fn main() {
    Codegen::out_dir("buffer_gen")
        .schema(
            SchemaBuilder::new("Model")
                .field("timestamp", U64)
                .field("name", STRING)
                .field("value", I64),
        )
        .schema(
            SchemaBuilder::new("Config")
                .field("field", STRING)
                .field("value", STRING),
        )
        .schema(
            SchemaBuilder::new("Output")
                .field("field", STRING)
                .field("value", I64)
                .field("pct_99", I64)
                .field("pct_90", I64),
        )
        .gen()
        .expect("buffer gen error");
}
