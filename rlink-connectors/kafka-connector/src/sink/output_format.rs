use rdkafka::ClientConfig;
use rlink::api::element::Record;
use rlink::api::function::{Context, Function, OutputFormat};
use rlink::utils::thread::get_runtime;
use rlink::{api, utils};

use crate::sink::handover::Handover;
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
        self.handover = Some(Handover::new(
            self.get_name(),
            self.topic.as_str(),
            context.task_id.job_id(),
            context.task_id.task_number(),
            self.buffer_size,
        ));

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
        self.handover.as_ref().unwrap().produce(record);
    }

    fn close(&mut self) -> api::Result<()> {
        Ok(())
    }
}
