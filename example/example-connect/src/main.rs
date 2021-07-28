#[macro_use]
extern crate rlink_derive;
#[macro_use]
extern crate log;

mod app;
mod co_connect;
mod map_output;
mod percentile;

pub fn main() {
    rlink::core::env::execute(crate::app::ConnectStreamApp1 {});
}
