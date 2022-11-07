#[macro_use]
extern crate rlink_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate async_trait;

mod app;
mod co_connect;
mod map_output;
mod percentile;

#[tokio::main]
async fn main() {
    rlink::core::env::execute(app::ConnectStreamApp1 {}).await;
}
