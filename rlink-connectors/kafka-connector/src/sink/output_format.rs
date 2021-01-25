use rdkafka::ClientConfig;
use rlink::api::element::Record;
use rlink::api::function::{Context, Function, OutputFormat};
use rlink::channel::handover::Handover;
use rlink::metrics::Tag;
use rlink::utils::thread::get_runtime;
use rlink::{api, utils};

use crate::sink::producer::KafkaProducerThread;

#[derive(Function)]
pub struct KafkaOutputFormat {
    client_config: ClientConfig,
    topic: String,

    buffer_size: usize,
    handover: Option<Handover>,
}

impl KafkaOutputFormat {
    pub fn new(client_config: ClientConfig, topic: String, buffer_size: usize) -> Self {
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
            Tag("topic".to_string(), self.topic.to_string()),
            Tag(
                "job_id".to_string(),
                format!("{}", context.task_id.job_id().0),
            ),
            Tag(
                "task_number".to_string(),
                format!("{}", context.task_id.task_number()),
            ),
        ];
        self.handover = Some(Handover::new(self.get_name(), tags, self.buffer_size));

        let topic = self.topic.clone();
        let client_config = self.client_config.clone();
        let handover = self.handover.as_ref().unwrap().clone();
        utils::thread::spawn("kafka-sink-block", move || {
            get_runtime().block_on(async {
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
