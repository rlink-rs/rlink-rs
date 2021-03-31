use std::borrow::BorrowMut;
use std::convert::TryFrom;

use bytes::{Buf, BufMut, BytesMut};

use crate::api::element::{Element, Serde};
use crate::api::runtime::{ChannelKey, JobId, TaskId};

pub(crate) mod client;
pub(crate) mod server;

pub(crate) use client::run_subscribe;
pub(crate) use client::subscribe;
pub(crate) use server::publish;
pub(crate) use server::Server;

const BODY_LEN: usize = 20;

#[derive(Clone, Debug)]
pub struct ElementRequest {
    channel_key: ChannelKey,
    batch_pull_size: u16,
    batch_id: u16,
}

impl Into<BytesMut> for ElementRequest {
    fn into(self) -> BytesMut {
        static HEADER_LEN: usize = 4usize;
        static PACKAGE_LEN: usize = HEADER_LEN + BODY_LEN;

        let mut buffer = BytesMut::with_capacity(PACKAGE_LEN);
        buffer.put_u32(BODY_LEN as u32); // (4 + 2 + 2) + (4 + 2 + 2) + 2 + 2
        buffer.put_u32(self.channel_key.source_task_id.job_id.0);
        buffer.put_u16(self.channel_key.source_task_id.task_number);
        buffer.put_u16(self.channel_key.source_task_id.num_tasks);
        buffer.put_u32(self.channel_key.target_task_id.job_id.0);
        buffer.put_u16(self.channel_key.target_task_id.task_number);
        buffer.put_u16(self.channel_key.target_task_id.num_tasks);
        buffer.put_u16(self.batch_pull_size);
        buffer.put_u16(self.batch_id);

        if buffer.len() != PACKAGE_LEN {
            unreachable!();
        }

        buffer
    }
}

impl TryFrom<BytesMut> for ElementRequest {
    type Error = anyhow::Error;

    fn try_from(mut buffer: BytesMut) -> Result<Self, Self::Error> {
        let len = buffer.get_u32(); // skip header length
        if len as usize != BODY_LEN {
            return Err(anyhow!(
                "Illegal request body length, expect {}, found {}",
                BODY_LEN,
                len
            ));
        }

        let source_task_id = {
            let job_id = buffer.get_u32();
            let task_number = buffer.get_u16();
            let num_tasks = buffer.get_u16();
            TaskId {
                job_id: JobId(job_id),
                task_number,
                num_tasks,
            }
        };

        let target_task_id = {
            let job_id = buffer.get_u32();
            let task_number = buffer.get_u16();
            let num_tasks = buffer.get_u16();
            TaskId {
                job_id: JobId(job_id),
                task_number,
                num_tasks,
            }
        };

        let batch_pull_size = buffer.get_u16();
        let batch_id = buffer.get_u16();

        Ok(ElementRequest {
            channel_key: ChannelKey {
                source_task_id,
                target_task_id,
            },
            batch_pull_size,
            batch_id,
        })
    }
}

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
    /// the target channel is closed, then send a package with the `Empty` code
    NoService = 4,
    // /// parse the Request error, then send a package with the `ParseErr` code
    // ParseErr = 5,
    // /// read the Request error, then send a package with the `ReadErr` code
    // ReadErr = 6,
}

impl From<u8> for ResponseCode {
    fn from(v: u8) -> Self {
        match v {
            1 => ResponseCode::Ok,
            2 => ResponseCode::BatchFinish,
            3 => ResponseCode::Empty,
            4 => ResponseCode::NoService,
            _ => ResponseCode::Unknown,
        }
    }
}

// impl TryFrom<u8> for ResponseCode {
//     type Error = anyhow::Error;
//
//     fn try_from(v: u8) -> Result<Self, Self::Error> {
//         match v {
//             1 => Ok(ResponseCode::Ok),
//             2 => Ok(ResponseCode::BatchFinish),
//             3 => Ok(ResponseCode::Empty),
//             4 => Ok(ResponseCode::NoService),
//             _ => Err(anyhow!("unknown code {}", v)),
//         }
//     }
// }

impl std::fmt::Display for ResponseCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseCode::Ok => write!(f, "OK"),
            ResponseCode::BatchFinish => write!(f, "BatchFinish"),
            ResponseCode::Empty => write!(f, "Empty"),
            ResponseCode::NoService => write!(f, "NoService"),
            ResponseCode::Unknown => write!(f, "Unknown"),
        }
    }
}

#[derive(Debug)]
pub struct ElementResponse {
    code: ResponseCode,
    element: Option<Element>,
}

impl ElementResponse {
    pub fn ok(element: Element) -> Self {
        ElementResponse {
            code: ResponseCode::Ok,
            element: Some(element),
        }
    }

    pub fn end(code: ResponseCode) -> Self {
        ElementResponse {
            code,
            element: None,
        }
    }
}

impl Into<BytesMut> for ElementResponse {
    fn into(self) -> BytesMut {
        let ElementResponse { code, element } = self;
        let element = match code {
            ResponseCode::Ok => element.unwrap(),
            _ => Element::new_stream_status(1, false),
        };

        static HEADER_LEN: usize = (4 + 1) as usize;
        let element_len = element.capacity();
        let package_len = HEADER_LEN + element_len;

        let mut buffer = bytes::BytesMut::with_capacity(package_len);
        buffer.put_u32(element_len as u32 + 1); // (code + body).length
        buffer.put_u8(code as u8);

        element.serialize(buffer.borrow_mut());

        if buffer.len() != package_len {
            unreachable!();
        }

        buffer
    }
}

impl TryFrom<BytesMut> for ElementResponse {
    type Error = anyhow::Error;

    fn try_from(mut buffer: BytesMut) -> Result<Self, Self::Error> {
        let _len = buffer.get_u32();

        let code_value = buffer.get_u8();
        let code = ResponseCode::from(code_value);
        if let ResponseCode::Unknown = code {
            return Err(anyhow!("found unknown code {}", code_value));
        }

        let element = Element::deserialize(buffer.borrow_mut());
        let element = match code {
            ResponseCode::Ok => Some(element),
            _ => None,
        };

        Ok(ElementResponse { code, element })
    }
}
