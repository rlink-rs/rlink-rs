use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use elasticsearch::http::headers::HeaderMap;
use elasticsearch::http::request::JsonBody;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::http::Url;
use elasticsearch::{BulkParts, Elasticsearch};
use rlink::api::element::Record;
use rlink::api::function::{Context, Function, OutputFormat};
use rlink::channel::mb;
use rlink::metrics::Tag;
use rlink::utils;
use rlink::utils::get_runtime;
use rlink::utils::handover::Handover;
use serde_json::Value;
use thiserror::Error;

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

pub trait ElasticsearchConverter: Debug + Send + Sync {
    fn to_json(&self, record: &mut Record) -> ElasticsearchModel;
}

#[derive(Debug, Function)]
pub struct ElasticsearchOutputFormat {
    address: String,
    headers: HashMap<String, String>,

    builder: Arc<Box<dyn ElasticsearchConverter>>,
    handover: Option<Handover>,
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
            handover: None,
        }
    }
}

impl OutputFormat for ElasticsearchOutputFormat {
    fn open(&mut self, context: &Context) {
        let tags = vec![
            Tag("job_id".to_string(), context.task_id.job_id().0.to_string()),
            Tag(
                "task_number".to_string(),
                context.task_id.task_number().to_string(),
            ),
        ];
        self.handover = Some(Handover::new(self.get_name(), tags, 100000, mb(10)));

        let mut write_thead = ElasticsearchWriteThread::new(
            self.address.as_str(),
            self.headers.clone(),
            self.handover.as_ref().unwrap().clone(),
            3000,
        )
        .expect("build elasticsearch connection error");

        let convert = self.builder.clone();
        utils::spawn("elastic-sink-block", move || {
            get_runtime().block_on(async {
                write_thead.run(convert, 5).await;
            });
        });
    }

    fn write_record(&mut self, record: Record) {
        self.handover.as_ref().unwrap().produce_always(record);
    }

    fn close(&mut self) {}
}

#[derive(Clone)]
pub struct ElasticsearchWriteThread {
    client: Elasticsearch,
    batch_size: usize,
    handover: Handover,
}

impl ElasticsearchWriteThread {
    pub fn new(
        address: &str,
        headers: HashMap<String, String>,
        handover: Handover,
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
            handover,
        })
    }

    pub async fn run(
        &mut self,
        converters: Arc<Box<dyn ElasticsearchConverter>>,
        parallelism: usize,
    ) {
        let mut join_handlers = Vec::new();
        for _ in 0..parallelism {
            let mut self_clone = self.clone();
            let converter = converters.clone();

            let handler = tokio::spawn(async move {
                self_clone.run0(converter).await;
            });

            join_handlers.push(handler);
        }

        for handler in join_handlers {
            handler.await.unwrap();
        }
    }

    pub async fn run0(&mut self, converter: Arc<Box<dyn ElasticsearchConverter>>) {
        loop {
            match self.batch_send(&converter).await {
                Ok(len) => {
                    if len == 0 {
                        tokio::time::delay_for(Duration::from_secs(1)).await;
                    }
                }
                Err(e) => {
                    error!("write elasticsearch error. {}", e);
                    tokio::time::delay_for(Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn batch_send(
        &self,
        converter: &Box<dyn ElasticsearchConverter>,
    ) -> Result<usize, Box<dyn std::error::Error + Send>> {
        let mut bulk_bodies = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            match self.handover.poll_next() {
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
                Err(_e) => {
                    break;
                }
            }
        }

        let len = bulk_bodies.len();
        self.flush(bulk_bodies).await.map_err(|e| {
            let err = std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e));
            let source: Box<dyn std::error::Error + Send> = Box::new(err);
            source
        })?;

        Ok(len)
    }

    async fn flush(
        &self,
        body_bulk: Vec<JsonBody<Value>>,
    ) -> Result<bool, Box<dyn std::error::Error>> {
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
            let err = std::io::Error::new(std::io::ErrorKind::Other, "");
            let source: Box<dyn std::error::Error + Send> = Box::new(err);
            Err(source)
        } else {
            Ok(true)
        }
    }
}

#[derive(Error, Debug)]
#[error("boxed source")]
pub struct BoxedSource {
    #[source]
    source: Box<dyn std::error::Error + Send + 'static>,
}
