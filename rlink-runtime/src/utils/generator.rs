use crate::utils::date_time::current_timestamp_millis;

pub fn gen_with_ts() -> String {
    format!("{}", current_timestamp_millis())
}
