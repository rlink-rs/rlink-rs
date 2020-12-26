// #[macro_use]
// extern crate serde_json;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate rlink_derive;

pub mod elasticsearch_sink;

pub static ES_DATA_TYPES: [u8; 2] = [
    // topic
    rlink::api::element::types::BYTES,
    // body
    rlink::api::element::types::BYTES,
];
