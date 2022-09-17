use rdkafka::ClientConfig;
use rlink::channel::named_channel;
use rlink::channel::sender::ChannelSender;
use rlink::core;
use rlink::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use rlink::core::element::{Element, Record};
use rlink::core::function::{Context, NamedFunction, OutputFormat};
use rlink::metrics::Tag;

use crate::sink::producer::KafkaProducerThread;

#[derive(NamedFunction)]
pub struct KafkaOutputFormat {
    client_config: ClientConfig,
    topic: Option<String>,

    buffer_size: usize,
    handover: Option<ChannelSender<Record>>,
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

#[async_trait]
impl OutputFormat for KafkaOutputFormat {
    async fn open(&mut self, context: &Context) -> core::Result<()> {
        let mut tags = context.task_id.to_tags();
        tags.push(Tag::new(
            "topic",
            self.topic.as_ref().map(|x| x.as_str()).unwrap_or(""),
        ));

        let (sender, receiver) = named_channel(self.name(), tags, self.buffer_size);
        self.handover = Some(sender);

        let topic = self.topic.clone();
        let client_config = self.client_config.clone();
        tokio::spawn(async move {
            let mut kafka_consumer = KafkaProducerThread::new(topic, client_config, receiver);
            kafka_consumer.run().await;
        });

        Ok(())
    }

    async fn write_element(&mut self, element: Element) {
        self.handover
            .as_ref()
            .unwrap()
            .send(element.into_record())
            .await
            .unwrap();
    }

    async fn close(&mut self) -> core::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl CheckpointFunction for KafkaOutputFormat {
    async fn initialize_state(
        &mut self,
        _context: &FunctionSnapshotContext,
        _handle: &Option<CheckpointHandle>,
    ) {
    }

    async fn snapshot_state(
        &mut self,
        _context: &FunctionSnapshotContext,
    ) -> Option<CheckpointHandle> {
        None
    }
}
