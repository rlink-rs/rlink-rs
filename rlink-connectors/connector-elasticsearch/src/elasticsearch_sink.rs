use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use elasticsearch::http::headers::HeaderMap;
use elasticsearch::http::request::JsonBody;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::http::Url;
use elasticsearch::{BulkParts, Elasticsearch};
use rlink::channel::receiver::ChannelReceiver;
use rlink::channel::sender::ChannelSender;
use rlink::channel::{named_channel, TryRecvError};
use rlink::core;
use rlink::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use rlink::core::element::{Element, FnSchema, Record};
use rlink::core::function::{Context, NamedFunction, OutputFormat};
use serde_json::Value;

pub struct ElasticsearchModel {
    pub index: String,
    pub es_type: &'static str,
    pub body: Value,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Index {
    index: HashMap<String, String>,
}

impl Index {
    pub fn new() -> Self {
        Index {
            index: HashMap::new(),
        }
    }

    pub fn set_index(&mut self, index_value: String) {
        self.index.insert("_index".to_string(), index_value);
    }

    pub fn set_type(&mut self, type_value: String) {
        self.index.insert("_type".to_string(), type_value);
    }

    pub fn to_json(&self) -> Result<Value, serde_json::Error> {
        serde_json::to_value(self)
    }
}

pub trait ElasticsearchConverter: Send + Sync {
    fn to_json(&self, record: &mut Record) -> ElasticsearchModel;
}

#[derive(NamedFunction)]
pub struct ElasticsearchOutputFormat {
    address: String,
    headers: HashMap<String, String>,

    builder: Arc<Box<dyn ElasticsearchConverter>>,
    sender: Option<ChannelSender<Record>>,
}

impl ElasticsearchOutputFormat {
    pub fn new(
        address: &str,
        headers: HashMap<String, String>,
        builder: Box<dyn ElasticsearchConverter>,
    ) -> Self {
        ElasticsearchOutputFormat {
            address: address.to_string(),
            headers,
            builder: Arc::new(builder),
            sender: None,
        }
    }
}

#[async_trait]
impl OutputFormat for ElasticsearchOutputFormat {
    async fn open(&mut self, context: &Context) -> core::Result<()> {
        let (sender, receiver) = named_channel(self.name(), context.task_id.to_tags(), 10000);
        self.sender = Some(sender);

        let mut write_thead = ElasticsearchWriteThread::new(
            self.address.as_str(),
            self.headers.clone(),
            receiver,
            3000,
        )
        .expect("build elasticsearch connection error");

        let convert = self.builder.clone();
        tokio::spawn(async move {
            write_thead.run(convert).await;
        });

        Ok(())
    }

    async fn write_element(&mut self, element: Element) {
        self.sender
            .as_ref()
            .unwrap()
            .send(element.into_record())
            .await
            .unwrap();
    }

    async fn close(&mut self) -> core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::Empty
    }
}

#[async_trait]
impl CheckpointFunction for ElasticsearchOutputFormat {
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

pub struct ElasticsearchWriteThread {
    client: Elasticsearch,
    batch_size: usize,
    receiver: ChannelReceiver<Record>,
}

impl ElasticsearchWriteThread {
    pub fn new(
        address: &str,
        headers: HashMap<String, String>,
        receiver: ChannelReceiver<Record>,
        batch_size: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut header_map = HeaderMap::new();
        if headers.contains_key("stoken") {
            let val = headers.get("stoken").unwrap();
            header_map.insert("stoken", val.as_str().parse().unwrap());
        }

        let url = Url::parse(address)?;
        let conn_pool = SingleNodeConnectionPool::new(url);
        let transport = TransportBuilder::new(conn_pool)
            .headers(header_map)
            .build()?;
        let client = Elasticsearch::new(transport);

        Ok(ElasticsearchWriteThread {
            client,
            batch_size,
            receiver,
        })
    }

    pub async fn run(&mut self, converters: Arc<Box<dyn ElasticsearchConverter>>) {
        let converter = converters.clone();
        self.run0(converter).await;
    }

    pub async fn run0(&mut self, converter: Arc<Box<dyn ElasticsearchConverter>>) {
        loop {
            match self.batch_send(&converter).await {
                Ok(len) => {
                    if len == 0 {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
                Err(e) => {
                    error!("write elasticsearch error. {}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn batch_send(
        &mut self,
        converter: &Box<dyn ElasticsearchConverter>,
    ) -> anyhow::Result<usize> {
        let mut bulk_bodies = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            match self.receiver.try_recv() {
                Ok(mut record) => {
                    let ElasticsearchModel {
                        index,
                        es_type,
                        body,
                    } = converter.to_json(record.borrow_mut());

                    let mut index_model = Index::new();
                    index_model.set_index(index.clone());
                    index_model.set_type(es_type.to_string());
                    bulk_bodies.push(JsonBody::new(index_model.to_json().unwrap()));

                    bulk_bodies.push(JsonBody::new(body));
                }
                Err(TryRecvError::Empty) => {
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    return Err(anyhow!("clickhouse channel has been disconnected"));
                }
            }
        }

        let len = bulk_bodies.len();
        self.flush(bulk_bodies).await?;

        Ok(len)
    }

    async fn flush(&self, body_bulk: Vec<JsonBody<Value>>) -> anyhow::Result<bool> {
        if body_bulk.len() == 0 {
            return Ok(true);
        }
        let response = self
            .client
            .bulk(BulkParts::None)
            .body(body_bulk)
            .send()
            .await?;
        let response_body = response.json::<Value>().await?;
        let errors = response_body["errors"]
            .as_bool()
            .ok_or(anyhow!("no errors field in es response"))?;

        if errors {
            Err(anyhow!("elasticsearch send error, {}", response_body))
        } else {
            Ok(true)
        }
    }
}
