use std::str::FromStr;

use log::LevelFilter;
use log::LevelFilter::Warn;
use log4rs::append::console::{ConsoleAppender, Target};
use log4rs::append::rolling_file::policy::compound::roll::fixed_window::FixedWindowRoller;
use log4rs::append::rolling_file::policy::compound::trigger::size::SizeTrigger;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::append::Append;
use log4rs::config::{Appender, Config, Logger, Root};
use log4rs::encode::pattern::PatternEncoder;

use crate::runtime::ClusterMode;
use crate::utils::process::get_work_space;

/// init log4r
/// cluster_mode: for appender build
/// level value: ["OFF", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"], ignore ascii case
pub(crate) fn init_log(cluster_mode: &ClusterMode, level: &str) {
    let console = match cluster_mode {
        ClusterMode::Local => true,
        ClusterMode::Standalone => false,
        ClusterMode::YARN => true,
    };

    let default_level = LevelFilter::from_str(level).expect("can not parse log level");

    let encoder =
        PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S%.3f)} {level} [{thread}] {target} - {m}{n}");

    let (name, appender) = if !console {
        ("rolling_file", create_rolling_file_appender(encoder))
    } else {
        ("console", create_console_appender(encoder))
    };

    let config = Config::builder()
        .appender(Appender::builder().build(name, appender))
        .logger(Logger::builder().build("actix_web::middleware::logger", Warn))
        .logger(Logger::builder().build("clickhouse_rs", Warn))
        .build(Root::builder().appender(name).build(default_level))
        .unwrap();

    println!("{:?}", &config);
    log4rs::init_config(config).unwrap();
}

fn create_console_appender(encoder: PatternEncoder) -> Box<dyn Append> {
    let stdout = ConsoleAppender::builder()
        .target(Target::Stdout)
        .encoder(Box::new(encoder))
        .build();
    let appender: Box<dyn Append> = Box::new(stdout);
    appender
}

fn create_rolling_file_appender(encoder: PatternEncoder) -> Box<dyn Append> {
    let cur_dir = get_work_space();
    let path = cur_dir.join("task-manager.log");
    let roll_path = path.to_str().unwrap().to_string() + ".{}";

    let trigger = SizeTrigger::new(50 * 1024 * 1024);

    let roll = FixedWindowRoller::builder()
        .base(1)
        .build(roll_path.as_str(), 20)
        .expect("log roll error");

    let policy = CompoundPolicy::new(Box::new(trigger), Box::new(roll));

    let rolling_file = RollingFileAppender::builder()
        .encoder(Box::new(encoder))
        .append(true)
        .build(path, Box::new(policy))
        .unwrap();
    let appender: Box<dyn Append> = Box::new(rolling_file);
    appender
}
