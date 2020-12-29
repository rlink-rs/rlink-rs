use crate::utils::date_time::current_timestamp_millis;

pub fn gen_id() -> String {
    format!("{}", current_timestamp_millis())
}
