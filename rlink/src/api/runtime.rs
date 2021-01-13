#[derive(Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Default)]
pub struct OperatorId(pub u32);

#[derive(Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Default)]
pub struct JobId(pub u32);

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
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Default)]
pub(crate) struct ChannelKey {
    pub(crate) source_task_id: TaskId,
    pub(crate) target_task_id: TaskId,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Default)]
pub struct CheckpointId(pub u64);

impl CheckpointId {
    #[inline]
    pub fn is_default(&self) -> bool {
        self.0 == 0
    }
}
