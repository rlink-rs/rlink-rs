pub mod date_time;
pub mod fs;
pub mod generator;
pub mod hash;
pub mod http;
pub mod ip;
pub mod panic;
pub mod process;
pub mod thread;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");

lazy_static! {
    pub static ref EMPTY_SLICE: &'static [u8] = &[];
    pub static ref EMPTY_VEC: Vec<u8> = Vec::with_capacity(0);
}
