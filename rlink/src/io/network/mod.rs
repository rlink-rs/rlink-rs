use bytes::{Buf, BufMut, BytesMut};

pub(crate) mod client;
pub(crate) mod server;

pub(crate) use client::run_subscribe;
pub(crate) use client::subscribe;
pub(crate) use server::publish;
pub(crate) use server::Server;

use crate::api::runtime::TaskId;
use crate::io::ChannelKey;

#[derive(Clone, Debug)]
pub struct ElementRequest {
    channel_key: ChannelKey,
    batch_pull_size: u16,
}

impl Into<BytesMut> for ElementRequest {
    fn into(self) -> BytesMut {
        let mut buffer = BytesMut::with_capacity(4 + 18);
        buffer.put_u32(12); // (4 + 2 + 2) + (4 + 2 + 2) + 2
        buffer.put_u32(self.channel_key.source_task_id.job_id);
        buffer.put_u16(self.channel_key.source_task_id.task_number);
        buffer.put_u16(self.channel_key.source_task_id.num_tasks);
        buffer.put_u32(self.channel_key.target_task_id.job_id);
        buffer.put_u16(self.channel_key.target_task_id.task_number);
        buffer.put_u16(self.channel_key.target_task_id.num_tasks);
        buffer.put_u16(self.batch_pull_size);

        buffer
    }
}

impl From<BytesMut> for ElementRequest {
    fn from(mut buffer: BytesMut) -> Self {
        let source_task_id = {
            let job_id = buffer.get_u32();
            let task_number = buffer.get_u16();
            let num_tasks = buffer.get_u16();
            TaskId {
                job_id,
                task_number,
                num_tasks,
            }
        };

        let target_task_id = {
            let job_id = buffer.get_u32();
            let task_number = buffer.get_u16();
            let num_tasks = buffer.get_u16();
            TaskId {
                job_id,
                task_number,
                num_tasks,
            }
        };

        let batch_pull_size = buffer.get_u16();

        ElementRequest {
            channel_key: ChannelKey {
                source_task_id,
                target_task_id,
            },
            batch_pull_size,
        }
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

impl std::fmt::Display for ResponseCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

// pub(crate) struct ElementResponse {}

pub(crate) trait ServerHandler
where
    Self: Sync + Send,
{
    fn on_request(&self);
}
