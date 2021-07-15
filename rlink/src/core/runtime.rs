use bytes::{Buf, BufMut, BytesMut};

use crate::core::element::Serde;
use crate::metrics::Tag;
use std::ops::Deref;

#[derive(
    Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Default, Ord, PartialOrd,
)]
pub struct OperatorId(pub u32);

#[derive(
    Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Default, Ord, PartialOrd,
)]
pub struct JobId(pub u32);

impl From<OperatorId> for JobId {
    fn from(operator_id: OperatorId) -> Self {
        JobId(operator_id.0)
    }
}

impl Deref for JobId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Default)]
pub struct TaskId {
    pub(crate) job_id: JobId,
    /// total number tasks in the Job. same as `parallelism`
    pub(crate) task_number: u16,
    pub(crate) num_tasks: u16,
}

impl TaskId {
    pub fn job_id(&self) -> JobId {
        self.job_id
    }
    pub fn task_number(&self) -> u16 {
        self.task_number
    }
    pub fn num_tasks(&self) -> u16 {
        self.num_tasks
    }

    pub fn is_default(&self) -> bool {
        self.job_id.0 == 0 && self.task_number == 0 && self.num_tasks == 0
    }

    pub fn to_tags(&self) -> Vec<Tag> {
        vec![
            Tag::new("job_id", self.job_id.0),
            Tag::new("task_number", self.task_number),
        ]
    }
}

impl Serde for TaskId {
    fn capacity(&self) -> usize {
        4 + 2 + 2
    }

    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_u32(self.job_id.0);
        bytes.put_u16(self.task_number);
        bytes.put_u16(self.num_tasks);
    }

    fn deserialize(bytes: &mut BytesMut) -> Self {
        let job_id = bytes.get_u32();
        let task_number = bytes.get_u16();
        let num_tasks = bytes.get_u16();
        TaskId {
            job_id: JobId(job_id),
            task_number,
            num_tasks,
        }
    }
}

/// mark where the data is now and where did it come from
///
///                  ┌──────────────────┐
/// Source Task ────►│     channel      ├────► Target Task
///                  └──────────────────┘
///
#[derive(Copy, Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash, Default)]
pub(crate) struct ChannelKey {
    pub(crate) source_task_id: TaskId,
    pub(crate) target_task_id: TaskId,
}

impl ChannelKey {
    pub fn to_tags(&self) -> Vec<Tag> {
        vec![
            Tag::new("source_job_id", self.source_task_id.job_id.0),
            Tag::new("source_task_number", self.source_task_id.task_number),
            Tag::new("target_job_id", self.target_task_id.job_id.0),
            Tag::new("target_task_number", self.target_task_id.task_number),
        ]
    }
}

impl Serde for ChannelKey {
    fn capacity(&self) -> usize {
        8 + 8
    }

    fn serialize(&self, bytes: &mut BytesMut) {
        self.source_task_id.serialize(bytes);
        self.target_task_id.serialize(bytes);
    }

    fn deserialize(bytes: &mut BytesMut) -> Self {
        let source_task_id = TaskId::deserialize(bytes);
        let target_task_id = TaskId::deserialize(bytes);

        ChannelKey {
            source_task_id,
            target_task_id,
        }
    }
}

#[derive(
    Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Default,
)]
pub struct CheckpointId(pub u64);

impl CheckpointId {
    #[inline]
    pub fn is_default(&self) -> bool {
        self.0 == 0
    }
}
