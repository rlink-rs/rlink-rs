use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use actix_web::http::{header, Method};
use actix_web::web::Data;
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use rand::prelude::StdRng;
use rand::Rng;

use crate::channel::{bounded, Sender};
use crate::core::cluster::StdResponse;
use crate::metrics::metric_handle;
use crate::utils::fs::read_binary;
use crate::utils::http::server::{as_ok_json, page_not_found};

pub(crate) async fn web_launch(context: Arc<crate::runtime::context::Context>) -> String {
    let (tx, mut rx) = bounded(1);

    std::thread::spawn(move || {
        actix_web::rt::System::new().block_on(async move {
            let ip = context.bind_ip.clone();
            let web_context = Arc::new(WebContext { context });
            serve_with_rand_port(web_context, ip, tx).await;
        });
    });

    let bind_addr: SocketAddr = rx.recv().await.unwrap();
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
    let mut rng: StdRng = rand::SeedableRng::from_entropy();
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
    let server_context = web_context.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(Data::from(server_context.clone()))
            .service(web::resource("/api/threads").route(web::get().to(get_thread_infos)))
            .service(
                web::resource("/api/client/log/enable").route(web::get().to(enable_client_log)),
            )
            .service(
                web::resource("/api/client/log/disable").route(web::get().to(disable_client_log)),
            )
            .service(
                web::resource("/api/server/log/enable").route(web::get().to(enable_server_log)),
            )
            .service(
                web::resource("/api/server/log/disable").route(web::get().to(disable_server_log)),
            )
            .service(web::resource("/api/metrics").route(web::get().to(metrics)))
            .default_service(web::route().to(default_handler))
    })
    .bind(*bind_addr)?;

    bind_addr_tx.send(*bind_addr).await.unwrap();
    server.run().await.map_err(|e| anyhow!(e))
}

async fn default_handler(req: HttpRequest, context: Data<WebContext>) -> HttpResponse {
    if req.method() == Method::GET && !req.path().starts_with("/api/") {
        static_file(req, context).await
    } else {
        page_not_found()
    }
}

async fn metrics(_context: Data<WebContext>) -> HttpResponse {
    let render = metric_handle().await.render();
    HttpResponse::Ok().body(render)
}

async fn enable_client_log(_context: Data<WebContext>) -> HttpResponse {
    crate::pub_sub::network::client::enable_log();
    as_ok_json(&StdResponse::ok(Some(true)))
}

async fn disable_client_log(_context: Data<WebContext>) -> HttpResponse {
    crate::pub_sub::network::client::disable_log();
    as_ok_json(&StdResponse::ok(Some(false)))
}

async fn enable_server_log(_context: Data<WebContext>) -> HttpResponse {
    crate::pub_sub::network::server::enable_log();
    as_ok_json(&StdResponse::ok(Some(true)))
}

async fn disable_server_log(_context: Data<WebContext>) -> HttpResponse {
    crate::pub_sub::network::server::disable_log();
    as_ok_json(&StdResponse::ok(Some(false)))
}

async fn get_thread_infos(_context: Data<WebContext>) -> HttpResponse {
    let c = crate::utils::thread::get_thread_infos();
    as_ok_json(&StdResponse::ok(Some(c)))
}

async fn static_file(req: HttpRequest, context: Data<WebContext>) -> HttpResponse {
    let path = {
        let mut path = req.path();
        if path.is_empty() || "/".eq(path) {
            path = "/index.html";
        };

        &path[1..path.len()]
    };

    let static_file_path = {
        let path = PathBuf::from(path);

        let dashboard_path = context.context.dashboard_path.as_str();
        let base_path = PathBuf::from(dashboard_path);

        base_path.join(path)
    };

    let ext = {
        let Some(ext_pos) = path.rfind(".") else {
            return page_not_found();
        };
        &path[ext_pos + 1..]
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
        Ok(context) => HttpResponse::Ok()
            .insert_header((header::CONTENT_TYPE, context_type))
            .body(context),
        Err(e) => {
            error!(
                "static file not found. file path: {:?}, error: {}",
                static_file_path, e
            );
            page_not_found()
        }
    }
}
