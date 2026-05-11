use std::net::SocketAddr;
use std::ops::Deref;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use actix_web::error;
use actix_web::http::{header, Method};
use actix_web::web::Data;
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};

use crate::channel::{bounded, Sender};
use crate::core::checkpoint::Checkpoint;
use crate::core::cluster::{MetadataStorageType, StdResponse};
use crate::core::runtime::ManagerStatus;
use crate::dag::metadata::DagMetadata;
use crate::metrics::metric_handle;
use crate::metrics::worker_proxy::collect_worker_metrics;
use crate::runtime::coordinator::checkpoint_manager::CheckpointManager;
use crate::runtime::HeartbeatRequest;
use crate::storage::metadata::{MetadataStorage, TMetadataStorage};
use crate::utils::fs::read_binary;
use crate::utils::http::server::{as_ok_json, page_not_found};

pub(crate) async fn web_launch(
    context: Arc<crate::runtime::context::Context>,
    metadata_mode: MetadataStorageType,
    checkpoint_manager: CheckpointManager,
    dag_metadata: DagMetadata,
) -> String {
    let (tx, mut rx) = bounded(1);

    std::thread::spawn(move || {
        actix_web::rt::System::new().block_on(async move {
            let ip = context.bind_ip.clone();
            let web_context = Arc::new(WebContext {
                context,
                metadata_mode,
                checkpoint_manager,
                dag_metadata,
            });
            serve_with_rand_port(web_context, ip, tx).await;
        });
    });

    let bind_addr: SocketAddr = rx.recv().await.unwrap();
    format!("http://{}", bind_addr.to_string())
}

struct WebContext {
    context: Arc<crate::runtime::context::Context>,
    metadata_mode: MetadataStorageType,
    checkpoint_manager: CheckpointManager,
    dag_metadata: DagMetadata,
}

async fn serve_with_rand_port(
    web_context: Arc<WebContext>,
    bind_id: String,
    bind_addr_tx: Sender<SocketAddr>,
) {
    let mut rng: StdRng = SeedableRng::from_entropy();
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
            .service(web::resource("/api/context").route(web::get().to(get_context)))
            .service(
                web::resource("/api/cluster_metadata").route(web::get().to(get_cluster_metadata)),
            )
            .service(web::resource("/api/checkpoints").route(web::get().to(get_checkpoint)))
            .service(web::resource("/api/dag_metadata").route(web::get().to(get_dag_metadata)))
            .service(web::resource("/api/dag/stream_graph").route(web::get().to(get_stream_graph)))
            .service(web::resource("/api/dag/job_graph").route(web::get().to(get_job_graph)))
            .service(
                web::resource("/api/dag/execution_graph").route(web::get().to(get_execution_graph)),
            )
            .service(web::resource("/api/threads").route(web::get().to(get_thread_infos)))
            .service(web::resource("/api/metrics").route(web::get().to(metrics)))
            .service(web::resource("/api/heartbeat").route(web::post().to(heartbeat)))
            .service(web::resource("/api/checkpoint").route(web::post().to(checkpoint)))
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

async fn metrics(context: Data<WebContext>) -> HttpResponse {
    let worker_renders = collect_worker_metrics(
        !context.context.cluster_mode.is_local(),
        context.metadata_mode.clone(),
    )
    .await;

    let render = metric_handle().await.render();

    let render = format!("{}\n{}\n", worker_renders, render);
    HttpResponse::Ok().body(render)
}

async fn get_context(context: Data<WebContext>) -> HttpResponse {
    let c = context.context.deref().clone();
    as_ok_json(&StdResponse::ok(Some(c)))
}

async fn get_cluster_metadata(context: Data<WebContext>) -> HttpResponse {
    let metadata_storage = MetadataStorage::new(&context.metadata_mode);
    let cluster_descriptor = metadata_storage.load().await.unwrap();
    as_ok_json(&StdResponse::ok(Some(cluster_descriptor)))
}

async fn get_checkpoint(context: Data<WebContext>) -> HttpResponse {
    let cks = context.checkpoint_manager.get().await;
    as_ok_json(&StdResponse::ok(Some(cks)))
}

async fn get_dag_metadata(context: Data<WebContext>) -> HttpResponse {
    let json_dag = context.dag_metadata.clone();
    as_ok_json(&StdResponse::ok(Some(json_dag)))
}

async fn get_stream_graph(context: Data<WebContext>) -> HttpResponse {
    let json_dag = context.dag_metadata.stream_graph().clone();
    as_ok_json(&StdResponse::ok(Some(json_dag)))
}

async fn get_job_graph(context: Data<WebContext>) -> HttpResponse {
    let json_dag = context.dag_metadata.job_graph().clone();
    as_ok_json(&StdResponse::ok(Some(json_dag)))
}

async fn get_execution_graph(context: Data<WebContext>) -> HttpResponse {
    let json_dag = context.dag_metadata.execution_graph().clone();
    as_ok_json(&StdResponse::ok(Some(json_dag)))
}

async fn get_thread_infos(_context: Data<WebContext>) -> HttpResponse {
    let c = crate::utils::thread::get_thread_infos();
    as_ok_json(&StdResponse::ok(Some(c)))
}

async fn heartbeat(body: web::Bytes, context: Data<WebContext>) -> actix_web::Result<HttpResponse> {
    let HeartbeatRequest {
        task_manager_id,
        change_items,
    } = serde_json::from_slice(&body).map_err(error::ErrorBadRequest)?;

    debug!(
        "<heartbeat> from {}, items: {:?}",
        task_manager_id, change_items
    );

    let metadata_storage = MetadataStorage::new(&context.metadata_mode);
    let coordinator_status = metadata_storage
        .update_worker_status(task_manager_id, change_items, ManagerStatus::Registered)
        .await;

    let resp: StdResponse<ManagerStatus> = coordinator_status.into();
    Ok(as_ok_json(&resp))
}

async fn checkpoint(
    body: web::Bytes,
    context: Data<WebContext>,
) -> actix_web::Result<HttpResponse> {
    let ck_model: Checkpoint = serde_json::from_slice(&body).map_err(error::ErrorBadRequest)?;

    let ck_manager = &context.checkpoint_manager;
    debug!("submit checkpoint to coordinator. {:?}", &ck_model);
    let resp = match ck_manager.apply(ck_model) {
        Ok(_) => "ok",
        Err(e) => {
            error!("submit checkpoint error. {}", e);
            "error"
        }
    };

    Ok(as_ok_json(&StdResponse::ok(Some(resp.to_string()))))
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
