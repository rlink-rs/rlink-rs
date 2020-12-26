#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;

use log4rs::append::console::{ConsoleAppender, Target};
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;

pub mod config;
pub mod controller;
pub mod job;
pub mod server;
pub mod utils;

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    init_log();
    info!("bootstrap");

    server::main().await
}

pub fn init_log() {
    let level = log::LevelFilter::Info;

    let encoder =
        PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S%.3f)} {level} [{thread}] {target} - {m}{n}");
    let stderr = ConsoleAppender::builder()
        .target(Target::Stderr)
        .encoder(Box::new(encoder))
        .build();

    let config = Config::builder()
        .appender(Appender::builder().build("stderr", Box::new(stderr)))
        .build(Root::builder().appender("stderr").build(level))
        .unwrap();

    log4rs::init_config(config).unwrap();
}
