use bytes::buf::Buf;
use bytes::buf::BufExt;
use hyper::{Body, Client, Request};
use serde::Serialize;

use crate::utils::thread::get_runtime;

pub fn post_sync<T>(
    url: String,
    body: String,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    T: Serialize + serde::de::DeserializeOwned + 'static,
{
    get_runtime().block_on(post(url, body))
}

pub async fn post<T>(
    url: String,
    body: String,
) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
where
    T: Serialize + serde::de::DeserializeOwned + 'static,
{
    let client = Client::new();

    let req = Request::builder()
        .method("POST")
        .uri(url.as_str())
        .header("Content-Type", "application/json")
        .body(Body::from(body))
        .expect("request builder");
    let res = client.request(req).await?;

    // asynchronously aggregate the chunks of the body
    let result = hyper::body::aggregate(res).await?;
    let result_json = serde_json::from_reader(result.reader())?;

    Ok(result_json)
}

pub fn get_sync(url: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let url = url.to_string();
    get_runtime().block_on(get(url.as_str()))
}

pub async fn get(url: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::new();

    let req = Request::builder()
        .method("GET")
        .uri(url)
        // .header("Content-Type", "application/json")
        .body(Body::default())?;
    let res = client.request(req).await?;

    // asynchronously aggregate the chunks of the body
    let result = hyper::body::to_bytes(res).await?;

    let bs = result.bytes();
    let s = String::from_utf8(bs.to_vec())?;

    Ok(s)
}
