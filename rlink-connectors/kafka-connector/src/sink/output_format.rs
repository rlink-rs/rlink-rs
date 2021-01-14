use rdkafka::ClientConfig;
use rlink::api::element::Record;
use rlink::api::function::{Context, Function, OutputFormat};
use rlink::utils;
use rlink::utils::get_runtime;

use crate::sink::handover::Handover;
use crate::sink::producer::KafkaProducerThread;

#[derive(Function)]
pub struct KafkaOutputFormat {
    client_config: ClientConfig,
    topic: String,
    handover: Option<Handover>,
}

impl KafkaOutputFormat {
    pub fn new(client_config: ClientConfig, topic: String) -> Self {
        KafkaOutputFormat {
            client_config,
            topic,
            handover: None,
        }
    }
}

impl OutputFormat for KafkaOutputFormat {
    fn open(&mut self, context: &Context) {
        self.handover = Some(Handover::new(
            self.get_name(),
            self.topic.as_str(),
            context.task_id.job_id(),
            context.task_id.task_number(),
        ));

        let topic = self.topic.clone();
        let client_config = self.client_config.clone();
        let handover = self.handover.as_ref().unwrap().clone();
        utils::spawn("kafka-sink-block", move || {
            get_runtime().block_on(async {
                let mut kafka_consumer = KafkaProducerThread::new(topic, client_config, handover);
                kafka_consumer.run().await;
            });
        });
    }

    fn write_record(&mut self, record: Record) {
        self.handover.as_ref().unwrap().produce(record);
    }

    fn close(&mut self) {}
}
