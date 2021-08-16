#[macro_use]
extern crate rlink_derive;

pub mod config_input_format;
pub mod gen_record;
pub mod rand_input_format;

pub mod buffer_gen {
    include!(concat!(env!("OUT_DIR"), "/buffer_gen/mod.rs"));
}
