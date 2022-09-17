use mysql_async::prelude::*;
use mysql_async::Params;

use crate::core::checkpoint::{Checkpoint, CheckpointHandle};
use crate::core::runtime::{CheckpointId, JobId, OperatorId, TaskId};
use crate::storage::checkpoint::{CheckpointEntity, TCheckpointStorage};
use crate::utils::date_time::{current_timestamp, fmt_date_time};

const DEFAULT_TABLE_NAME: &'static str = "rlink_ck";

pub struct MySqlCheckpointStorage {
    url: String,
    table: String,
}

impl MySqlCheckpointStorage {
    pub fn new(url: String, table: Option<String>) -> Self {
        MySqlCheckpointStorage {
            url: url.to_string(),
            table: table.unwrap_or(DEFAULT_TABLE_NAME.to_string()),
        }
    }

    async fn query<P>(&mut self, sql: String, p: P) -> anyhow::Result<Vec<Checkpoint>>
    where
        P: Into<Params> + Send,
    {
        let pool = mysql_async::Pool::new(self.url.as_str());
        let mut conn = pool.get_conn().await?;

        let selected_payments = sql
            .with(p)
            .map(
                &mut conn,
                |(
                    job_id,
                    task_number,
                    num_tasks,
                    operator_id,
                    checkpoint_id,
                    completed_checkpoint_id,
                    handle,
                )| {
                    let completed_checkpoint_id = if completed_checkpoint_id == 0 {
                        None
                    } else {
                        Some(CheckpointId(completed_checkpoint_id))
                    };

                    Checkpoint {
                        operator_id: OperatorId(operator_id),
                        task_id: TaskId {
                            job_id: JobId(job_id),
                            task_number,
                            num_tasks,
                        },
                        checkpoint_id: CheckpointId(checkpoint_id),
                        completed_checkpoint_id,
                        handle: CheckpointHandle { handle },
                    }
                },
            )
            .await?;

        info!("checkpoint load success");
        Ok(selected_payments)
    }
}

#[async_trait]
impl TCheckpointStorage for MySqlCheckpointStorage {
    async fn save(&mut self, ck: CheckpointEntity) -> anyhow::Result<()> {
        let CheckpointEntity {
            application_name,
            application_id,
            checkpoint_id,
            finish_cks,
            ttl,
            ..
        } = ck;

        let pool = mysql_async::Pool::new(self.url.as_str());
        let mut conn = pool.get_conn().await?;

        r"
insert into rlink_ck
  (application_name, application_id, job_id, task_number, num_tasks, operator_id, checkpoint_id, completed_checkpoint_id, handle, create_time)
values
  (:application_name, :application_id, :job_id, :task_number, :num_tasks, :operator_id, :checkpoint_id, :completed_checkpoint_id, :handle, :create_time)"
            .replace("rlink_ck", self.table.as_str())
            .with(finish_cks.iter().map(|p| {
                let completed_checkpoint_id = p.completed_checkpoint_id.unwrap_or_default();
                params! {
                    "application_name" => application_name.as_str(),
                    "application_id" => application_id.as_str(),
                    "job_id" => p.task_id.job_id.0,
                    "task_number" => p.task_id.task_number,
                    "num_tasks" => p.task_id.num_tasks,
                    "operator_id" => p.operator_id.0,
                    "checkpoint_id" => checkpoint_id.0,
                    "completed_checkpoint_id" => completed_checkpoint_id.0,
                    "handle" => &p.handle.handle,
                    "create_time" => fmt_date_time(current_timestamp(), "%Y-%m-%d %T"),
                }
            }))
            .batch(&mut conn)
            .await?;

        if checkpoint_id.0 < ttl {
            return Ok(());
        }

        let checkpoint_id_ttl = checkpoint_id.0 - ttl;

        let _n: Option<usize> = r"
delete
from rlink_ck
where application_name = :application_name
  and application_id = :application_id
  and checkpoint_id < :checkpoint_id"
            .replace("rlink_ck", self.table.as_str())
            .with(params! {
                "application_name" => application_name.as_str(),
                "application_id" => application_id.as_str(),
                "checkpoint_id" => checkpoint_id_ttl
            })
            .first(&mut conn)
            .await?;

        info!(
            "checkpoint save success, application_name={:?}, checkpoint_id={:?}",
            application_name, checkpoint_id
        );
        Ok(())
    }

    async fn load(
        &mut self,
        application_name: &str,
        application_id: &str,
    ) -> anyhow::Result<Vec<Checkpoint>> {
        self.query(
            r"
SELECT  ck.job_id, ck.task_number, ck.num_tasks, ck.operator_id,
        ck.checkpoint_id, ck.completed_checkpoint_id, ck.handle
from rlink_ck as ck
        inner join (
    SELECT max(checkpoint_id) as checkpoint_id
    from rlink_ck
    where application_name = :application_name
    and application_id = :application_id
) as t on t.checkpoint_id = ck.checkpoint_id
where ck.application_name = :application_name
and ck.application_id = :application_id"
                .replace("rlink_ck", self.table.as_str()),
            params! {
               "application_name" => application_name,
               "application_id" => application_id,
            },
        )
        .await
    }

    async fn load_by_checkpoint_id(
        &mut self,
        application_name: &str,
        application_id: &str,
        checkpoint_id: CheckpointId,
    ) -> anyhow::Result<Vec<Checkpoint>> {
        self.query(
            r"
SELECT  ck.job_id, ck.task_number, ck.num_tasks, ck.operator_id, 
        ck.checkpoint_id, ck.completed_checkpoint_id, ck.handle
from rlink_ck as ck
where ck.application_name = :application_name
    and ck.application_id = :application_id
    and ck.checkpoint_id = :checkpoint_id"
                .replace("rlink_ck", self.table.as_str()),
            params! {
                "application_name" => application_name,
                "application_id" => application_id,
                "checkpoint_id" => checkpoint_id.0
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::core::checkpoint::{Checkpoint, CheckpointHandle};
    use crate::core::runtime::{CheckpointId, JobId, OperatorId, TaskId};
    use crate::storage::checkpoint::mysql_checkpoint_storage::MySqlCheckpointStorage;
    use crate::storage::checkpoint::{CheckpointEntity, TCheckpointStorage};

    #[tokio::test]
    pub async fn mysql_storage_test() {
        let application_name = "test_app_name";
        let application_id = "test_app_id";
        let job_id = JobId(5u32);
        let task_id0 = TaskId {
            job_id,
            task_number: 0,
            num_tasks: 2,
        };
        let task_id1 = TaskId {
            job_id,
            task_number: 1,
            num_tasks: 2,
        };
        let operator_id = OperatorId(1);
        let checkpoint_id = CheckpointId(crate::utils::date_time::current_timestamp_millis());

        let mut mysql_storage = MySqlCheckpointStorage::new(
            "mysql://root:mysqlpw@localhost:55000/rlink".to_string(),
            None,
        );
        let ck = CheckpointEntity::new(
            application_name.to_string(),
            application_id.to_string(),
            checkpoint_id,
            vec![
                Checkpoint {
                    operator_id,
                    task_id: task_id0,
                    checkpoint_id,
                    completed_checkpoint_id: None,
                    handle: CheckpointHandle {
                        handle: "h0".to_string(),
                    },
                },
                Checkpoint {
                    operator_id,
                    task_id: task_id1,
                    checkpoint_id,
                    completed_checkpoint_id: None,
                    handle: CheckpointHandle {
                        handle: "h1".to_string(),
                    },
                },
            ],
            1000 * 60 * 60 * 24 * 3,
        );
        mysql_storage.save(ck).await.unwrap();

        let cks = mysql_storage
            .load(application_name, application_id)
            .await
            .unwrap();

        for ck in &cks {
            println!("{:?}", ck);
        }
        assert_eq!(cks.len(), 2);

        let cks = mysql_storage
            .load_by_checkpoint_id(application_name, application_id, checkpoint_id)
            .await
            .unwrap();
        assert_eq!(cks.len(), 2);
    }
}
