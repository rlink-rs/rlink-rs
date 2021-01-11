use mysql::prelude::*;
use mysql::*;

use crate::api::checkpoint::{Checkpoint, CheckpointHandle};
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
        job_id: u32,
        checkpoint_id: u64,
        finish_cks: Vec<Checkpoint>,
        ttl: u64,
    ) -> anyhow::Result<()> {
        let pool = Pool::new(self.url.as_str())?;

        let mut conn = pool.get_conn()?;
        conn.exec_batch(
            r"
insert into rlink_cks 
  (application_name, application_id, job_id, checkpoint_id, task_num, handle, create_time)
values 
  (:application_name, :application_id, :job_id, :checkpoint_id, :task_num, :handle, :create_time)",
            finish_cks.iter().map(|p| {
                params! {
                    "application_name" => application_name,
                    "application_id" => application_id,
                    "job_id" => job_id,
                    "checkpoint_id" => checkpoint_id,
                    "task_num" => p.task_num,
                    "handle" => &p.handle.handle,
                    "create_time" => fmt_date_time(current_timestamp(), "%Y-%m-%d %T"),
                }
            }),
        )?;

        if checkpoint_id < ttl {
            return Ok(());
        }

        let checkpoint_id_ttl = checkpoint_id - ttl;
        let _n: Option<usize> = conn.exec_first(
            r"
delete
from rlink_cks
where application_name = :application_name
  and job_id = :job_id
  and application_id = :application_id
  and checkpoint_id < :checkpoint_id",
            params! {
                "application_name" => application_name,
                "job_id"=> job_id,
                "application_id" => application_id,
                "checkpoint_id" => checkpoint_id_ttl
            },
        )?;

        info!(
            "checkpoint save success, job_id={}, checkpoint_id={}",
            job_id, checkpoint_id
        );
        Ok(())
    }

    fn load(&mut self, application_name: &str, job_id: u32) -> anyhow::Result<Vec<Checkpoint>> {
        let pool = Pool::new(self.url.as_str())?;

        let mut conn = pool.get_conn()?;

        let stmt = conn.prep(
            r"
SELECT cks.job_id, cks.checkpoint_id, cks.task_num, cks.handle
from rlink_cks as cks
         inner join (
    SELECT max(checkpoint_id) as checkpoint_id
    from rlink_cks
    where application_name = :application_name
      and job_id = :job_id
) as t on t.checkpoint_id = cks.checkpoint_id
where cks.application_name = :application_name
  and cks.job_id = :job_id",
        )?;

        let selected_payments = conn.exec_map(
            &stmt,
            params! { "application_name" => application_name, "job_id" => job_id },
            |(job_id, checkpoint_id, task_num, handle)| Checkpoint {
                job_id,
                task_num,
                checkpoint_id,
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
    use crate::storage::checkpoint::mysql_checkpoint_storage::MySqlCheckpointStorage;
    use crate::storage::checkpoint::CheckpointStorage;

    #[test]
    pub fn mysql_storage_test() {
        let checkpoint_id = crate::utils::date_time::current_timestamp_millis();

        let mut mysql_storage =
            MySqlCheckpointStorage::new("mysql://rlink:123456@localhost:3304/rlink");
        mysql_storage
            .save(
                "abc",
                "def",
                5u32,
                checkpoint_id,
                vec![
                    Checkpoint {
                        job_id: 5u32,
                        task_num: 1,
                        checkpoint_id,
                        handle: CheckpointHandle {
                            handle: "ha".to_string(),
                        },
                    },
                    Checkpoint {
                        job_id: 5u32,
                        task_num: 2,
                        checkpoint_id,
                        handle: CheckpointHandle {
                            handle: "hx".to_string(),
                        },
                    },
                ],
                1000 * 60 * 60 * 24 * 3,
            )
            .unwrap();

        let cks = mysql_storage.load("abc", 5u32).unwrap();

        for ck in cks {
            println!("{:?}", ck);
        }
    }
}
