use chrono::{DateTime, Local, Utc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const FMT_TIME: &str = "%T%z";
pub const FMT_DATE_TIME: &str = "%Y-%m-%d %T";
pub const FMT_DATE_TIME_1: &str = "%Y-%m-%dT%T%.3f";

/// current timestamp
pub fn current_timestamp() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch")
}

/// current timestamp as millis
pub fn current_timestamp_millis() -> u64 {
    current_timestamp().as_millis() as u64
}

/// format timestamp to string
pub fn fmt_date_time(dur: Duration, fmt: &str) -> String {
    let utl_dt: DateTime<Utc> = (UNIX_EPOCH + dur).into();
    let local_dt = utl_dt.with_timezone(&Local);
    local_dt.format(fmt).to_string()
}

/// for timestamp debug print
pub fn timestamp_str(timestamp: u64) -> String {
    format!(
        "{}({})",
        fmt_date_time(Duration::from_millis(timestamp), FMT_DATE_TIME_1),
        timestamp
    )
}

#[cfg(test)]
mod tests {
    use crate::utils::date_time::{current_timestamp, fmt_date_time, FMT_DATE_TIME_1};

    #[test]
    pub fn fmt_date_time_test() {
        let s = fmt_date_time(current_timestamp(), FMT_DATE_TIME_1);
        println!("{}", s);
    }
}
