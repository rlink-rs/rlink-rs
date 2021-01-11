#[derive(Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Default)]
pub struct TaskId {
    pub(crate) job_id: u32,
    /// total number tasks in the chain. same as `parallelism`
    pub(crate) task_number: u16,
    pub(crate) num_tasks: u16,
}

impl TaskId {
    pub fn job_id(&self) -> u32 {
        self.job_id
    }
    pub fn task_number(&self) -> u16 {
        self.task_number
    }
    pub fn num_tasks(&self) -> u16 {
        self.num_tasks
    }
}
