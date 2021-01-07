use bytes::{Buf, BufMut, BytesMut};

use crate::dag::TaskId;
use crate::io::pub_sub::ChannelKey;

pub(crate) mod client;
pub(crate) mod server;

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

pub(crate) struct ElementResponse {}

pub(crate) trait ServerHandler
where
    Self: Sync + Send,
{
    fn on_request(&self);
}
