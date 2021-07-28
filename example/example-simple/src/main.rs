#[macro_use]
extern crate rlink_derive;

mod app;
mod filter;
mod mapper;

pub fn main() {
    rlink::core::env::execute(crate::app::SimpleStreamApp {});
}
