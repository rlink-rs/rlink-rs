use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use hyper::http::header;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response};
use hyper::{Server, StatusCode};
use rand::Rng;

use crate::channel::{bounded, Sender};
use crate::core::cluster::StdResponse;
use crate::utils::fs::read_binary;
use crate::utils::http::server::{as_ok_json, page_not_found};
use crate::utils::thread::async_runtime_multi;

pub(crate) fn web_launch(context: Arc<crate::runtime::context::Context>) -> String {
    let (tx, rx) = bounded(1);

    std::thread::Builder::new()
        .name("WebUI".to_string())
        .spawn(move || {
            async_runtime_multi("web", 2).block_on(async move {
                let ip = context.bind_ip.clone();
                let web_context = Arc::new(WebContext { context });
                serve_with_rand_port(web_context, ip, tx).await;
            });
        })
        .unwrap();

    let bind_addr: SocketAddr = rx.recv().unwrap();
    format!("http://{}", bind_addr.to_string())
}

struct WebContext {
    context: Arc<crate::runtime::context::Context>,
}

async fn serve_with_rand_port(
    web_context: Arc<WebContext>,
    bind_id: String,
    bind_addr_tx: Sender<SocketAddr>,
) {
    let mut rng = rand::thread_rng();
    for _ in 0..30 {
        let port = rng.gen_range(10000..30000);
        let address = format!("{}:{}", bind_id.as_str(), port);
        let socket_addr = SocketAddr::from_str(address.as_str()).unwrap();

        let serve_result = serve(web_context.clone(), &socket_addr, bind_addr_tx.clone()).await;
        match serve_result {
            Ok(_) => error!("server stop"),
            Err(e) => info!("try bind failure> {}", e),
        }
    }

    error!("no port can be bound");
}

async fn serve(
    web_context: Arc<WebContext>,
    bind_addr: &SocketAddr,
    bind_addr_tx: Sender<SocketAddr>,
) -> anyhow::Result<()> {
    // And a MakeService to handle each connection...
    let make_service = make_service_fn(move |_conn| {
        let web_context = web_context.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                let web_context = web_context.clone();
                route(req, web_context)
            }))
        }
    });

    // Then bind and serve...
    let server = Server::try_bind(bind_addr)?.serve(make_service);

    bind_addr_tx.send(bind_addr.clone()).unwrap();

    // And run forever...
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }

    Ok(())
}

async fn route(req: Request<Body>, web_context: Arc<WebContext>) -> anyhow::Result<Response<Body>> {
    let path = req.uri().path();
    let method = req.method();

    if path.starts_with("/api/") {
        if Method::GET.eq(method) {
            match path {
                "/api/threads" => get_thread_infos(req, web_context).await,
                "/api/client/log/enable" => enable_client_log(req, web_context).await,
                "/api/client/log/disable" => disable_client_log(req, web_context).await,
                "/api/server/log/enable" => enable_server_log(req, web_context).await,
                "/api/server/log/disable" => disable_server_log(req, web_context).await,
                _ => page_not_found().await,
            }
        } else {
            page_not_found().await
        }
    } else {
        if Method::GET.eq(method) {
            static_file(req, web_context).await
        } else {
            page_not_found().await
        }
    }
}

async fn enable_client_log(
    _req: Request<Body>,
    _context: Arc<WebContext>,
) -> anyhow::Result<Response<Body>> {
    crate::pub_sub::network::client::enable_log();
    as_ok_json(&StdResponse::ok(Some(true)))
}

async fn disable_client_log(
    _req: Request<Body>,
    _context: Arc<WebContext>,
) -> anyhow::Result<Response<Body>> {
    crate::pub_sub::network::client::disable_log();
    as_ok_json(&StdResponse::ok(Some(false)))
}

async fn enable_server_log(
    _req: Request<Body>,
    _context: Arc<WebContext>,
) -> anyhow::Result<Response<Body>> {
    crate::pub_sub::network::server::enable_log();
    as_ok_json(&StdResponse::ok(Some(true)))
}

async fn disable_server_log(
    _req: Request<Body>,
    _context: Arc<WebContext>,
) -> anyhow::Result<Response<Body>> {
    crate::pub_sub::network::server::disable_log();
    as_ok_json(&StdResponse::ok(Some(false)))
}

async fn get_thread_infos(
    _req: Request<Body>,
    _context: Arc<WebContext>,
) -> anyhow::Result<Response<Body>> {
    let c = crate::utils::thread::get_thread_infos();
    as_ok_json(&StdResponse::ok(Some(c)))
}

async fn static_file(
    req: Request<Body>,
    context: Arc<WebContext>,
) -> anyhow::Result<Response<Body>> {
    let path = {
        let mut path = req.uri().path();
        if path.is_empty() || "/".eq(path) {
            path = "/index.html";
        };

        &path[1..path.len()]
    };

    let static_file_path = {
        let path = PathBuf::from_str(path)?;

        let dashboard_path = context.context.dashboard_path.as_str();
        let base_path = PathBuf::from_str(dashboard_path)?;

        let n = base_path.join(path);
        n
    };

    let ext = {
        let ext_pos = path.rfind(".").ok_or(anyhow!("file ext name not found"))?;
        &path[ext_pos + 1..path.len()]
    };

    let context_type = match ext {
        "html" => "text/html; charset=utf-8",
        "js" => "application/javascript",
        "css" => "text/css",
        "ico" => "image/x-icon",
        "gif" => "image/gif",
        "png" => "image/png",
        "svg" => "image/svg+xml",
        "woff" => "application/font-woff",
        _ => "",
    };

    match read_binary(&static_file_path) {
        Ok(context) => Response::builder()
            .header(header::CONTENT_TYPE, context_type)
            .status(StatusCode::OK)
            .body(Body::from(context))
            .map_err(|e| anyhow!(e)),
        Err(e) => {
            error!(
                "static file not found. file path: {:?}, error: {}",
                static_file_path, e
            );
            page_not_found().await
        }
    }
}
