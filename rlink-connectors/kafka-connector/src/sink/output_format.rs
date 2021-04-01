use rdkafka::ClientConfig;
use rlink::api::element::Record;
use rlink::api::function::{Context, NamedFunction, OutputFormat};
use rlink::channel::utils::handover::Handover;
use rlink::metrics::Tag;
use rlink::utils::thread::async_runtime;
use rlink::{api, utils};

use crate::sink::producer::KafkaProducerThread;
use rlink::api::checkpoint::CheckpointFunction;

#[derive(NamedFunction)]
pub struct KafkaOutputFormat {
    client_config: ClientConfig,
    topic: Option<String>,

    buffer_size: usize,
    handover: Option<Handover>,
}

impl KafkaOutputFormat {
    pub fn new(client_config: ClientConfig, topic: Option<String>, buffer_size: usize) -> Self {
        KafkaOutputFormat {
            client_config,
            topic,
            buffer_size,
            handover: None,
        }
    }
}

impl OutputFormat for KafkaOutputFormat {
    fn open(&mut self, context: &Context) -> api::Result<()> {
        let tags = vec![
            Tag::from((
                "topic",
                self.topic.as_ref().map(|x| x.as_str()).unwrap_or(""),
            )),
            Tag::from(("job_id", context.task_id.job_id().0)),
            Tag::from(("task_number", context.task_id.task_number())),
        ];
        self.handover = Some(Handover::new(self.name(), tags, self.buffer_size));

        let topic = self.topic.clone();
        let client_config = self.client_config.clone();
        let handover = self.handover.as_ref().unwrap().clone();
        utils::thread::spawn("kafka-sink-block", move || {
            async_runtime("kafka_sink").block_on(async {
                let mut kafka_consumer = KafkaProducerThread::new(topic, client_config, handover);
                kafka_consumer.run().await;
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

impl CheckpointFunction for KafkaOutputFormat {}
