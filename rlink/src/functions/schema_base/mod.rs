pub mod key_selector;
pub mod print_output_format;
pub mod reduce;
pub mod timestamp_assigner;

pub trait FunctionSchema {
    fn schema_types(&self) -> Vec<u8>;
}
