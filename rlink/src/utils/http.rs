pub mod server {
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::http::header;
    use hyper::{Response, StatusCode};
    use serde::Serialize;

    pub fn as_ok_json<T>(t: &T) -> anyhow::Result<Response<Full<Bytes>>>
    where
        T: Serialize,
    {
        let json = serde_json::to_string(t).unwrap();
        return Response::builder()
            .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
            .status(StatusCode::OK)
            .body(Full::new(Bytes::from(json)))
            .map_err(|e| anyhow!(e));
    }

    pub async fn page_not_found() -> anyhow::Result<Response<Full<Bytes>>> {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("Page not found")))
            .map_err(|e| anyhow!(e))
    }
}

pub mod client {
    use bytes::Buf;
    use http_body_util::{BodyExt, Full};
    use hyper::body::Bytes;
    use hyper::Request;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::TokioExecutor;
    use serde::Serialize;

    pub async fn post<T>(
        url: String,
        body: String,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        T: Serialize + serde::de::DeserializeOwned + 'static,
    {
        request("POST", url, body).await
    }

    pub async fn put<T>(
        url: String,
        body: String,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        T: Serialize + serde::de::DeserializeOwned + 'static,
    {
        request("PUT", url, body).await
    }

    pub async fn request<T>(
        method: &'static str,
        url: String,
        body: String,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        T: Serialize + serde::de::DeserializeOwned + 'static,
    {
        let client = Client::builder(TokioExecutor::new()).build_http();

        let req = Request::builder()
            .method(method)
            .uri(url.as_str())
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(body)))
            .expect("request builder");
        let res = client.request(req).await?;

        let body = res.into_body().collect().await?;
        let result_json = serde_json::from_reader(body.aggregate().reader())?;

        Ok(result_json)
    }

    pub async fn get(url: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let client = Client::builder(TokioExecutor::new()).build_http();

        let req = Request::builder()
            .method("GET")
            .uri(url)
            .body(Full::new(Bytes::new()))?;
        let res = client.request(req).await?;

        let body = res.into_body().collect().await?;
        let bs = body.to_bytes().to_vec();
        let s = String::from_utf8(bs)?;

        Ok(s)
    }
}
