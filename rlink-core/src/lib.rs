#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate atomic_enum;

use crate::cluster::ManagerStatus;
use crate::properties::Properties;

pub mod channel;
pub mod cluster;
pub mod context;
pub mod error;
// pub mod graph;
pub mod cmd;
pub mod element;
pub mod env;
pub mod job;
pub mod metrics;
pub mod properties;

#[derive(
    Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Default, Ord, PartialOrd,
)]
pub struct JobId(pub u32);

impl std::ops::Deref for JobId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Default)]
pub struct TaskId {
    pub job_id: JobId,
    /// total number tasks in the Job. same as `parallelism`
    pub task_number: u16,
    pub num_tasks: u16,
}

/// mark where the data is now and where did it come from
///
///                  ┌──────────────────┐
/// Source Task ────►│     channel      ├────► Target Task
///                  └──────────────────┘
///
#[derive(Copy, Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash, Default)]
pub struct ChannelKey {
    pub source_task_id: TaskId,
    pub target_task_id: TaskId,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct HeartbeatRequest {
    pub task_manager_id: String,
    pub change_items: Vec<HeartbeatItem>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum HeartbeatItem {
    WorkerAddrs { web_address: String },
    HeartBeatStatus(HeartBeatStatus),
    TaskEnd { task_id: TaskId },
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
pub struct TaskDescriptor {
    pub task_id: TaskId,
    /// mark the task is `Terminated` status
    pub terminated: bool,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct WorkerManagerDescriptor {
    pub status: ManagerStatus,
    pub latest_heart_beat_ts: u64,
    pub latest_heart_beat_status: HeartBeatStatus,
    pub task_manager_id: String,
    // pub task_manager_address: String,
    pub web_address: String,
    pub task_descriptors: Vec<TaskDescriptor>,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct CoordinatorManagerDescriptor {
    /// `rlink` compile version
    pub version: String,
    pub application_id: String,
    pub application_properties: Properties,
    pub web_address: String,
    pub status: ManagerStatus,
    pub v_cores: u32,
    pub memory_mb: u32,
    pub num_task_managers: u32,
    pub uptime: u64,
    pub startup_number: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
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

    pub fn get_worker_manager_by_mid(
        &self,
        task_manager_id: &str,
    ) -> Option<WorkerManagerDescriptor> {
        for task_manager_descriptors in &self.worker_managers {
            if task_manager_descriptors.task_manager_id.eq(task_manager_id) {
                return Some(task_manager_descriptors.clone());
            }
        }

        None
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
            self.coordinator_manager.status = ManagerStatus::Terminated;
        } else if terminated_count > 0 {
            self.coordinator_manager.status = ManagerStatus::Terminating;
        }
    }

    pub fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}
