pub mod server {
    use hyper::http::header;
    use hyper::{Body, Response, StatusCode};
    use serde::Serialize;

    pub fn as_ok_json<T>(t: &T) -> anyhow::Result<Response<Body>>
    where
        T: Serialize,
    {
        let json = serde_json::to_string(t).unwrap();
        return Response::builder()
            .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
            .status(StatusCode::OK)
            .body(Body::from(json))
            .map_err(|e| anyhow!(e));
    }

    pub async fn page_not_found() -> anyhow::Result<Response<Body>> {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Page not found"))
            .map_err(|e| anyhow!(e))
    }
}

pub mod client {
    use bytes::buf::Buf;
    use hyper::{Body, Client, Request};
    use serde::Serialize;

    use crate::utils::thread::{async_runtime, async_runtime_single};

    pub fn post_sync<T>(
        url: String,
        body: String,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        T: Serialize + serde::de::DeserializeOwned + 'static,
    {
        async_runtime("http").block_on(post(url, body))
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
        async_runtime_single().block_on(get(url.as_str()))
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

        let bs = result.to_vec();
        let s = String::from_utf8(bs)?;

        Ok(s)
    }
}
