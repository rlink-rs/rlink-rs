use std::borrow::BorrowMut;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use clickhouse_rs::{ClientHandle, Options, Pool};
use rlink::api::checkpoint::CheckpointFunction;
use rlink::api::element::Record;
use rlink::api::function::{Context, NamedFunction, OutputFormat};
use rlink::channel::utils::handover::Handover;
use rlink::metrics::Tag;
use rlink::utils::thread::{async_runtime, async_sleep, async_spawn};
use rlink::{api, utils};

pub type CkBlock = clickhouse_rs::Block;

pub trait ClickhouseConverter: Send + Sync {
    fn create_batch(&self, batch_size: usize) -> Box<dyn ClickhouseBatch>;
}

pub trait ClickhouseBatch: Send + Sync {
    fn append(&mut self, record: Record);
    fn flush(&mut self) -> CkBlock;
}

#[derive(NamedFunction)]
pub struct ClickhouseSink {
    url: String,
    table: String,
    batch_size: usize,
    batch_timeout: Duration,
    tasks: usize,
    converter: Arc<Box<dyn ClickhouseConverter>>,
    handover: Option<Handover>,
}

impl ClickhouseSink {
    pub fn new(
        url: &str,
        table: &str,
        batch_size: usize,
        batch_timeout: Duration,
        tasks: usize,
        builder: Box<dyn ClickhouseConverter>,
    ) -> Self {
        ClickhouseSink {
            url: url.to_string(),
            table: table.to_string(),
            batch_size,
            batch_timeout,
            tasks,
            converter: Arc::new(builder),
            handover: None,
        }
    }
}

impl OutputFormat for ClickhouseSink {
    fn open(&mut self, context: &Context) -> api::Result<()> {
        let tags = vec![
            Tag::from(("job_id", context.task_id.job_id().0)),
            Tag::from(("task_number", context.task_id.task_number())),
        ];
        self.handover = Some(Handover::new(self.name(), tags, 10000));

        let urls: Vec<&str> = self.url.split(",").collect();
        let url = if urls.len() > 1 {
            urls.get(context.task_id.task_number() as usize % urls.len())
                .unwrap()
                .to_string()
        } else {
            self.url.to_string()
        };
        info!("location clickhouse database url:{} from {}", url, self.url);

        let mut task = ClickhouseSinkTask::new(
            url.as_str(),
            self.table.clone(),
            self.batch_size,
            self.batch_timeout,
            self.converter.clone(),
            self.handover.as_ref().unwrap().clone(),
        );
        let tasks = self.tasks;
        utils::thread::spawn("clickhouse-sink-block", move || {
            async_runtime("ck_sink").block_on(async {
                task.run(tasks).await;
            });
        });

        Ok(())
    }

    fn write_record(&mut self, record: Record) {
        self.handover.as_ref().unwrap().produce(record).unwrap();
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
}

impl CheckpointFunction for ClickhouseSink {}

#[derive(Clone)]
pub struct ClickhouseSinkTask {
    pool: Pool,
    table: String,
    batch_size: usize,
    batch_timeout: Duration,
    converter: Arc<Box<dyn ClickhouseConverter>>,
    handover: Handover,
}

impl ClickhouseSinkTask {
    pub fn new(
        url: &str,
        table: String,
        batch_size: usize,
        batch_timeout: Duration,
        builder: Arc<Box<dyn ClickhouseConverter>>,
        handover: Handover,
    ) -> Self {
        let opts = Options::from_str(url).expect("parse clickhouse url error");
        let pool = Pool::new(opts);
        ClickhouseSinkTask {
            pool,
            table,
            batch_size,
            batch_timeout,
            converter: builder,
            handover,
        }
    }

    pub async fn run(&mut self, tasks: usize) {
        let mut join_handlers = Vec::new();
        for _ in 0..tasks {
            let mut self_clone = self.clone();

            let handler = async_spawn(async move {
                match self_clone.run0().await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("run task error. {}", e);
                    }
                }
            });

            join_handlers.push(handler);
        }

        for handler in join_handlers {
            handler.await.unwrap();
        }
    }

    pub async fn run0(&mut self) -> anyhow::Result<()> {
        let mut client = self.pool.get_handle().await?;
        loop {
            match self.batch_send(client.borrow_mut()).await {
                Ok(len) => {
                    if len == 0 {
                        async_sleep(Duration::from_secs(1)).await;
                    }
                }
                Err(e) => {
                    error!("write clickhouse error. {}", e);

                    // todo reconnection
                    self.reconnection(client.borrow_mut()).await?;
                }
            }
        }
    }

    async fn reconnection(&mut self, client: &mut ClientHandle) -> anyhow::Result<()> {
        let mut err = None;
        for _ in 0..180 {
            async_sleep(Duration::from_secs(1)).await;
            match client.check_connection().await {
                Ok(_) => {
                    err = None;
                    break;
                }
                Err(e) => {
                    error!("reconnection error. {:?}", e);
                    err = Some(e)
                }
            }
        }

        match err {
            Some(e) => Err(anyhow::Error::from(e)),
            None => Ok(()),
        }
    }

    async fn batch_send(&mut self, client: &mut ClientHandle) -> anyhow::Result<usize> {
        let mut batch_block = self.converter.create_batch(self.batch_size);
        let begin_timestamp = utils::date_time::current_timestamp();
        let mut size = 0;
        for n in 0..self.batch_size {
            match self.handover.try_poll_next() {
                Ok(record) => {
                    batch_block.append(record);
                    size = n;
                }
                Err(_e) => {
                    async_sleep(Duration::from_millis(100)).await;
                    let current_timestamp = utils::date_time::current_timestamp();
                    if current_timestamp - begin_timestamp > self.batch_timeout {
                        break;
                    }
                }
            }
        }

        if size > 0 {
            let block = batch_block.flush();
            client.insert(self.table.as_str(), block).await?;
        }

        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use clickhouse_rs::Options;
    use std::str::FromStr;

    #[test]
    pub fn options_test() {
        let opt = Options::from_str(
            "tcp://rlink:123456@localhost:9000?keepalive=10s&connection_timeout=10s",
        )
        .unwrap();
        println!("{:?}", opt);
    }
}
