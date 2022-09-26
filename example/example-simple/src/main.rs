#[macro_use]
extern crate rlink_derive;
#[macro_use]
extern crate async_trait;

mod app;
mod filter;
mod mapper;

#[tokio::main]
async fn main() {
    rlink::core::env::execute(app::SimpleStreamApp {}).await;
}
