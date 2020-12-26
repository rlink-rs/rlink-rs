use crate::utils::date_time::current_timestamp_millis;
// use rand::prelude::*;

pub fn gen_id() -> String {
    format!("{}", current_timestamp_millis())

    // let mut rng = rand::thread_rng();
    // format!(
    //     "{}_{}",
    //     current_timestamp_millis(),
    //     rng.gen_range(1000, 9999)
    // )
}
