use mysql::prelude::*;
use mysql::*;

use crate::api::checkpoint::{Checkpoint, CheckpointHandle};
use crate::api::runtime::{CheckpointId, JobId, OperatorId, TaskId};
use crate::storage::checkpoint::CheckpointStorage;
use crate::utils::date_time::{current_timestamp, fmt_date_time};

#[derive(Debug)]
pub struct MySqlCheckpointStorage {
    url: String,
}

impl MySqlCheckpointStorage {
    pub fn new(url: &str) -> Self {
        MySqlCheckpointStorage {
            url: url.to_string(),
        }
    }
}

impl CheckpointStorage for MySqlCheckpointStorage {
    fn save(
        &mut self,
        application_name: &str,
        application_id: &str,
        checkpoint_id: CheckpointId,
        finish_cks: Vec<Checkpoint>,
        ttl: u64,
    ) -> anyhow::Result<()> {
        let pool = Pool::new(self.url.as_str())?;

        let mut conn = pool.get_conn()?;
        conn.exec_batch(
            r"
insert into rlink_ck 
  (application_name, application_id, job_id, task_number, num_tasks, operator_id, checkpoint_id, handle, create_time)
values 
  (:application_name, :application_id, :job_id, :task_number, :num_tasks, :operator_id, :checkpoint_id, :handle, :create_time)",
            finish_cks.iter().map(|p| {
                params! {
                    "application_name" => application_name,
                    "application_id" => application_id,
                    "job_id" => p.task_id.job_id.0,
                    "task_number" => p.task_id.task_number,
                    "num_tasks" => p.task_id.num_tasks,
                    "operator_id" => p.operator_id.0,
                    "checkpoint_id" => checkpoint_id.0,
                    "handle" => &p.handle.handle,
                    "create_time" => fmt_date_time(current_timestamp(), "%Y-%m-%d %T"),
                }
            }),
        )?;

        if checkpoint_id.0 < ttl {
            return Ok(());
        }

        let checkpoint_id_ttl = checkpoint_id.0 - ttl;
        let _n: Option<usize> = conn.exec_first(
            r"
delete
from rlink_ck
where application_name = :application_name
  and application_id = :application_id
  and checkpoint_id < :checkpoint_id",
            params! {
                "application_name" => application_name,
                "application_id" => application_id,
                "checkpoint_id" => checkpoint_id_ttl
            },
        )?;

        info!(
            "checkpoint save success, application_name={:?}, checkpoint_id={:?}",
            application_name, checkpoint_id
        );
        Ok(())
    }

    fn load(
        &mut self,
        application_name: &str,
        job_id: JobId,
        operator_id: OperatorId,
    ) -> anyhow::Result<Vec<Checkpoint>> {
        let pool = Pool::new(self.url.as_str())?;

        let mut conn = pool.get_conn()?;

        let stmt = conn.prep(
            r"
SELECT ck.job_id, ck.task_number, ck.num_tasks, ck.operator_id, ck.checkpoint_id, ck.handle
from rlink_ck as ck
         inner join (
    SELECT max(checkpoint_id) as checkpoint_id
    from rlink_ck
    where application_name = :application_name
      and job_id = :job_id
      and operator_id = :operator_id
) as t on t.checkpoint_id = ck.checkpoint_id
where ck.application_name = :application_name
  and ck.job_id = :job_id
  and ck.operator_id = :operator_id",
        )?;

        let selected_payments = conn.exec_map(
            &stmt,
            params! { "application_name" => application_name, "job_id" => job_id.0, "operator_id" => operator_id.0 },
            |(job_id, task_number, num_tasks, operator_id, checkpoint_id, handle)| Checkpoint {
                operator_id: OperatorId(operator_id),
                task_id: TaskId {
                    job_id: JobId(job_id),
                    task_number,
                    num_tasks,
                },
                checkpoint_id: CheckpointId(checkpoint_id),
                handle: CheckpointHandle { handle },
            },
        )?;

        info!("checkpoint load success");
        Ok(selected_payments)
    }
}

#[cfg(test)]
mod tests {
    use crate::api::checkpoint::{Checkpoint, CheckpointHandle};
    use crate::api::runtime::{CheckpointId, JobId, OperatorId, TaskId};
    use crate::storage::checkpoint::mysql_checkpoint_storage::MySqlCheckpointStorage;
    use crate::storage::checkpoint::CheckpointStorage;

    #[test]
    pub fn mysql_storage_test() {
        let job_id = JobId(5u32);
        let task_id = TaskId {
            job_id,
            task_number: 0,
            num_tasks: 5,
        };
        let operator_id = OperatorId(1);
        let checkpoint_id = CheckpointId(crate::utils::date_time::current_timestamp_millis());

        let mut mysql_storage =
            MySqlCheckpointStorage::new("mysql://rlink:123456@localhost:3304/rlink");
        mysql_storage
            .save(
                "abc",
                "def",
                checkpoint_id,
                vec![
                    Checkpoint {
                        operator_id,
                        task_id,
                        checkpoint_id,
                        handle: CheckpointHandle {
                            handle: "ha".to_string(),
                        },
                    },
                    Checkpoint {
                        operator_id,
                        task_id,
                        checkpoint_id,
                        handle: CheckpointHandle {
                            handle: "hx".to_string(),
                        },
                    },
                ],
                1000 * 60 * 60 * 24 * 3,
            )
            .unwrap();

        let cks = mysql_storage.load("abc", job_id).unwrap();

        for ck in cks {
            println!("{:?}", ck);
        }
    }
}
