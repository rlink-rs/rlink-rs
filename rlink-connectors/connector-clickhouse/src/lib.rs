#[macro_use]
extern crate log;
#[macro_use]
extern crate rlink_derive;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate anyhow;

pub mod clickhouse_sink;

use chrono::{DateTime, NaiveDateTime, TimeZone};
use chrono_tz::Asia;

pub type DateTimeShanghai = DateTime<chrono_tz::Tz>;
/// timestamp to DateTime<Tz>
/// 1. Asia::Shanghai.ymd(2020, 11, 11).and_hms(12, 0, 1);
/// 2. let naive_dt = NaiveDateTime::from_timestamp(1604995711, 0);
//     let dt: DateTime<chrono_tz::Tz> = Asia::Shanghai.from_utc_datetime(&naive_dt);
pub fn timestamp_to_tz(ts: u64) -> DateTimeShanghai {
    let d = std::time::Duration::from_millis(ts);

    let secs = d.as_secs();
    let nsecs = (ts - secs * 1000) as u32 * 1_000_000;
    let naive_dt = NaiveDateTime::from_timestamp_opt(secs as i64, nsecs).unwrap();
    let dt: DateTime<chrono_tz::Tz> = Asia::Shanghai.from_utc_datetime(&naive_dt);
    dt
}
