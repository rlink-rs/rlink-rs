use std::convert::TryFrom;
use std::ops::Deref;

use bytes::{Buf, BufMut, BytesMut};

use crate::core::checkpoint::CheckpointHandle;
use crate::core::element::Serde;
use crate::core::function::InputSplit;
use crate::core::properties::Properties;
use crate::metrics::Tag;

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

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct OperatorDescriptor {
    pub operator_id: OperatorId,
    pub checkpoint_id: CheckpointId,
    pub completed_checkpoint_id: Option<CheckpointId>,
    pub checkpoint_handle: Option<CheckpointHandle>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct TaskDescriptor {
    pub task_id: TaskId,
    pub operators: Vec<OperatorDescriptor>,
    pub input_split: InputSplit,
    pub daemon: bool,
    pub thread_id: String,
    /// mark the task is `Terminated` status
    pub terminated: bool,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq)]
pub enum ManagerStatus {
    /// Waiting for the TaskManager register
    Pending = 0,
    /// TaskManager has registered
    Registered = 1,
    /// TaskManager lost and try to recreate a new TaskManager
    Migration = 2,
    /// One of non-daemon Tasks terminated
    Terminating = 3,
    /// All Tasks terminated
    Terminated = 4,
}

impl ManagerStatus {
    pub fn is_terminating(&self) -> bool {
        match self {
            ManagerStatus::Terminating => true,
            _ => false,
        }
    }

    pub fn is_terminated(&self) -> bool {
        match self {
            ManagerStatus::Terminated => true,
            _ => false,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum HeartBeatStatus {
    Ok,
    Panic,
    End,
}

impl std::fmt::Display for HeartBeatStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HeartBeatStatus::Ok => write!(f, "ok"),
            HeartBeatStatus::Panic => write!(f, "panic"),
            HeartBeatStatus::End => write!(f, "end"),
        }
    }
}

impl<'a> TryFrom<&'a str> for HeartBeatStatus {
    type Error = anyhow::Error;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        match value {
            "ok" => Ok(HeartBeatStatus::Ok),
            "panic" => Ok(HeartBeatStatus::Panic),
            "end" => Ok(HeartBeatStatus::End),
            _ => Err(anyhow!("unrecognized status: {}", value)),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerManagerDescriptor {
    pub task_status: ManagerStatus,
    pub latest_heart_beat_ts: u64,
    pub latest_heart_beat_status: HeartBeatStatus,
    pub task_manager_id: String,
    pub task_manager_address: String,
    pub metrics_address: String,
    pub web_address: String,
    /// job tasks map: <job_id, Vec<TaskDescriptor>>
    pub task_descriptors: Vec<TaskDescriptor>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct CoordinatorManagerDescriptor {
    /// `rlink` compile version
    pub version: String,
    pub application_id: String,
    pub application_properties: Properties,
    // todo rename to web_address
    pub coordinator_address: String,
    pub metrics_address: String,
    pub coordinator_status: ManagerStatus,
    pub v_cores: u32,
    pub memory_mb: u32,
    pub num_task_managers: u32,
    pub uptime: u64,
    pub startup_number: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ClusterDescriptor {
    pub coordinator_manager: CoordinatorManagerDescriptor,
    pub worker_managers: Vec<WorkerManagerDescriptor>,
}

impl ClusterDescriptor {
    pub fn get_worker_manager(&self, task_id: &TaskId) -> Option<&WorkerManagerDescriptor> {
        self.worker_managers
            .iter()
            .find(|worker_manager_descriptor| {
                worker_manager_descriptor
                    .task_descriptors
                    .iter()
                    .find(|task_descriptor| task_descriptor.task_id.eq(task_id))
                    .is_some()
            })
    }

    pub fn flush_coordinator_status(&mut self) {
        let mut task_count = 0;
        let mut terminated_count = 0;

        for worker_manager in &self.worker_managers {
            for task in &worker_manager.task_descriptors {
                task_count += 1;
                if task.terminated {
                    terminated_count += 1;
                }
            }
        }

        if task_count == terminated_count {
            self.coordinator_manager.coordinator_status = ManagerStatus::Terminated;
        } else if terminated_count > 0 {
            self.coordinator_manager.coordinator_status = ManagerStatus::Terminating;
        }
    }

    pub fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}
