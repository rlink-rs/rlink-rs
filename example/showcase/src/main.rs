#[macro_use]
extern crate log;
#[macro_use]
extern crate rlink_derive;

mod app;
mod buffer_gen;

pub fn main() {
    rlink::api::env::execute("showcase", app::ConnectStreamApp1 {});
}
