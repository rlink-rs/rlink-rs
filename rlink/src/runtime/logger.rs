use std::path::PathBuf;

use log::LevelFilter;
use log::LevelFilter::Warn;
use log4rs::append::console::{ConsoleAppender, Target};
use log4rs::append::Append;
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;

pub(crate) fn init_log(log_config_path: Option<String>) -> anyhow::Result<()> {
    let config = match log_config_path {
        Some(log_config_path) => {
            let path = PathBuf::from(log_config_path);
            load_config_from_file(path)?
        }
        None => init_default()?,
    };

    println!("{:?}", &config);
    log4rs::init_config(config)?;

    Ok(())
}

fn load_config_from_file(path: PathBuf) -> anyhow::Result<Config> {
    log4rs::config::load_config_file(path, Default::default())
}

fn init_default() -> Result<Config, log4rs::config::runtime::ConfigErrors> {
    let name = "console";
    let default_level = LevelFilter::Info;
    let encoder =
        PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S%.3f)} {level} [{thread}] {target} - {m}{n}");
    let appender = create_console_appender(encoder);
    Config::builder()
        .appender(Appender::builder().build(name, appender))
        .logger(Logger::builder().build("actix_web::middleware::logger", Warn))
        .logger(Logger::builder().build("clickhouse_rs", Warn))
        .build(Root::builder().appender(name).build(default_level))
}

fn create_console_appender(encoder: PatternEncoder) -> Box<dyn Append> {
    let stdout = ConsoleAppender::builder()
        .target(Target::Stdout)
        .encoder(Box::new(encoder))
        .build();
    let appender: Box<dyn Append> = Box::new(stdout);
    appender
}
