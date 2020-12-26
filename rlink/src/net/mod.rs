use std::fmt::{Display, Formatter};

pub mod worker_client;
pub mod worker_client_pool;
pub mod worker_service;

/// Response code
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseCode {
    /// unknown code
    Unknown = 0,
    /// for per user data package
    Ok = 1,
    /// after the special batch, then send a finish batch package with the `BatchFinish` code
    BatchFinish = 2,
    /// there is no data in the channel, then send a package with the `Empty` code
    Empty = 3,
    /// parse the Request error, then send a package with the `ParseErr` code
    ParseErr = 4,
    /// read the Request error, then send a package with the `ReadErr` code
    ReadErr = 5,
}

impl From<u8> for ResponseCode {
    fn from(v: u8) -> Self {
        match v {
            1 => ResponseCode::Ok,
            2 => ResponseCode::BatchFinish,
            3 => ResponseCode::Empty,
            4 => ResponseCode::ParseErr,
            5 => ResponseCode::ReadErr,
            _ => ResponseCode::Unknown,
        }
    }
}

impl Display for ResponseCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseCode::Ok => write!(f, "OK"),
            ResponseCode::BatchFinish => write!(f, "BatchFinish"),
            ResponseCode::Empty => write!(f, "Empty"),
            ResponseCode::ParseErr => write!(f, "ParseErr"),
            ResponseCode::ReadErr => write!(f, "ReadErr"),
            ResponseCode::Unknown => write!(f, "Unknown"),
        }
    }
}
