#[macro_use]
extern crate rlink_derive;
#[macro_use]
extern crate log;

mod app;
mod co_connect;

pub fn main() {
    rlink::core::env::execute("rlink-connect", crate::app::ConnectStreamApp1 {});
}
