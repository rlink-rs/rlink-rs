#[macro_use]
extern crate log;
// #[macro_use]
// extern crate serde_derive;
#[macro_use]
extern crate rlink_derive;

mod buffer_gen;
mod job;

pub fn main() {
    rlink::api::env::execute("test", crate::job::join::MyStreamJob {});
}
