use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use actix_files::Files;
use actix_web::web::Data;
use actix_web::{middleware, web, App, Error, HttpResponse, HttpServer};
use rand::prelude::*;

use crate::api::checkpoint::Checkpoint;
use crate::api::cluster::MetadataStorageType;
use crate::api::cluster::{ResponseCode, StdResponse};
use crate::dag::metadata::DagMetadata;
use crate::runtime::coordinator::checkpoint_manager::CheckpointManager;
use crate::runtime::TaskManagerStatus;
use crate::storage::metadata::MetadataStorage;
use crate::storage::metadata::TMetadataStorage;
use crate::utils::VERSION;
use actix_web::http::header;

#[derive(Clone, Debug)]
pub(crate) struct WebContext {
    app_context: Arc<crate::runtime::context::Context>,
    metadata_mode: MetadataStorageType,
}

pub(crate) fn web_launch(
    context: Arc<crate::runtime::context::Context>,
    metadata_mode: MetadataStorageType,
    checkpoint_manager: CheckpointManager,
    dag_metadata: DagMetadata,
) -> String {
    let address: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let address_clone = address.clone();
    std::thread::Builder::new()
        .name("WebUI".to_string())
        .spawn(move || {
            serve_sync(
                context,
                metadata_mode,
                address_clone,
                checkpoint_manager,
                dag_metadata,
            );
        })
        .unwrap();

    loop {
        std::thread::sleep(std::time::Duration::from_millis(100));

        let address = address.lock().unwrap();
        if let Some(add) = &*address {
            return add.clone();
        }
    }
}

pub(crate) fn serve_sync(
    job_context: Arc<crate::runtime::context::Context>,
    metadata_mode: MetadataStorageType,
    address: Arc<Mutex<Option<String>>>,
    checkpoint_manager: CheckpointManager,
    dag_metadata: DagMetadata,
) {
    actix_rt::System::new("Coordinator Web UI")
        .block_on(serve(
            job_context,
            metadata_mode,
            address,
            checkpoint_manager,
            dag_metadata,
        ))
        .unwrap();
}

async fn serve(
    app_context: Arc<crate::runtime::context::Context>,
    metadata_mode: MetadataStorageType,
    rt_address: Arc<Mutex<Option<String>>>,
    checkpoint_manager: CheckpointManager,
    dag_metadata: DagMetadata,
) -> std::io::Result<()> {
    let context = WebContext {
        app_context,
        metadata_mode,
    };

    let ip = context.app_context.bind_ip.clone();

    let mut rng = rand::thread_rng();
    for _ in 0..30 {
        let port = rng.gen_range(10000, 30000);
        let address = format!("{}:{}", ip.as_str(), port);

        let asset_path = PathBuf::from(context.app_context.asset_path.as_str());

        let data = Data::new(context.clone());
        let data_ck_manager = Data::new(checkpoint_manager.clone());
        let dag_metadata = Data::new(dag_metadata.clone());
        let server = HttpServer::new(move || {
            let app = App::new()
                .app_data(data.clone())
                .app_data(data_ck_manager.clone())
                .app_data(dag_metadata.clone())
                .wrap(middleware::Logger::default())
                .wrap(middleware::DefaultHeaders::new().header("X-Version", VERSION))
                .service(web::resource("/api/heartbeat").route(web::post().to(heartbeat)))
                .service(web::resource("/api/context").route(web::get().to(get_context)))
                .service(web::resource("/api/metadata").route(web::get().to(get_metadata)))
                .service(web::resource("/api/dag_metadata").route(web::get().to(get_dag_metadata)))
                .service(web::resource("/api/checkpoint").route(web::post().to(checkpoint)))
                .service(web::resource("/api/checkpoints").route(web::get().to(get_checkpoint)))
                .service(
                    web::resource("/api/dag/stream_graph").route(web::get().to(get_stream_graph)),
                )
                .service(web::resource("/api/dag/job_graph").route(web::get().to(get_job_graph)))
                .service(
                    web::resource("/api/dag/execution_graph")
                        .route(web::get().to(get_execution_graph)),
                );

            if asset_path.exists() {
                app.service(
                    Files::new("/", asset_path.clone())
                        .prefer_utf8(true)
                        .index_file("index.html"),
                )
            } else {
                warn!("asset path not exist. can set `asset_path` to enable WebUI");
                app.service(
                    web::resource("/")
                        .wrap(
                            middleware::DefaultHeaders::new()
                                .header(header::CONTENT_TYPE, "text/html; charset=UTF-8"),
                        )
                        .route(web::get().to(index)),
                )
            }
        })
        .disable_signals()
        .workers(8)
        .bind(address.clone());

        match server {
            Ok(x) => {
                {
                    let mut rt_address = rt_address.lock().unwrap();
                    *rt_address = Some(format!("http://{}", address));
                }
                return x.run().await;
            }
            Err(_e) => {
                //ignore
            }
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        "port inuse",
    ))
}

fn index() -> HttpResponse {
    let html = r#"<html>
        <head><title>rlink-rs Home</title></head>
        <body>
            <h1>rlink api</h1>
            <ul>
                <li><a href="context">context</a></li>
                <li><a href="metadata">metadata</a></li>
                <li><a href="checkpoints">checkpoints</a></li>
                <li><a href="dag/stream_graph">dag:stream_graph</a></li>
                <li><a href="dag/job_graph">dag:job_graph</a></li>
                <li><a href="dag/execution_graph">dag:execution_graph</a></li>
                <li><a href="dag/physic_graph">dag:physic_graph</a></li>
            </ul>
        </body>
    </html>"#;

    HttpResponse::Ok().body(html)
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct HeartbeatModel {
    pub task_manager_id: String,
    pub task_manager_address: String,
    pub metrics_address: String,
    // ok, panic
    pub status: String,
}

pub(crate) async fn heartbeat(
    heartbeat_model: web::Json<HeartbeatModel>,
    context: Data<WebContext>,
) -> Result<HttpResponse, Error> {
    let metadata_storage = MetadataStorage::new(&context.metadata_mode);

    if !heartbeat_model.status.eq("ok") {
        error!(
            "heartbeat status: {}, model: {:?} ",
            heartbeat_model.status.as_str(),
            heartbeat_model
        );
    }

    metadata_storage
        .update_task_manager_status(
            heartbeat_model.task_manager_id.as_str(),
            heartbeat_model.task_manager_address.as_str(),
            TaskManagerStatus::Registered,
            heartbeat_model.metrics_address.as_str(),
        )
        .unwrap();

    let response = StdResponse::new(ResponseCode::OK, Some(true));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_context(context: Data<WebContext>) -> Result<HttpResponse, Error> {
    let response = StdResponse::new(ResponseCode::OK, Some(context.app_context.deref().clone()));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_metadata(context: Data<WebContext>) -> Result<HttpResponse, Error> {
    let metadata_storage = MetadataStorage::new(&context.metadata_mode);
    let job_descriptor = metadata_storage.load().unwrap();

    let response = StdResponse::new(ResponseCode::OK, Some(job_descriptor));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn checkpoint(
    ck_model: web::Json<Checkpoint>,
    ck_manager: Data<CheckpointManager>,
) -> Result<HttpResponse, Error> {
    debug!("submit checkpoint to coordinator. {:?}", &ck_model.0);
    let resp = match ck_manager.get_ref().apply(ck_model.0) {
        Ok(_) => "ok",
        Err(e) => {
            error!("submit checkpoint error. {}", e);
            "error"
        }
    };

    let response = StdResponse::new(ResponseCode::OK, Some(resp.to_string()));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_checkpoint(
    ck_manager: Data<CheckpointManager>,
) -> Result<HttpResponse, Error> {
    let cks = ck_manager.get_ref().get();

    let response = StdResponse::new(ResponseCode::OK, Some(cks));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_dag_metadata(
    dag_metadata: Data<DagMetadata>,
) -> Result<HttpResponse, Error> {
    let json_dag = dag_metadata.get_ref().clone();

    let response = StdResponse::new(ResponseCode::OK, Some(json_dag));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_stream_graph(
    dag_metadata: Data<DagMetadata>,
) -> Result<HttpResponse, Error> {
    let json_dag = dag_metadata.get_ref().stream_graph().clone();

    let response = StdResponse::new(ResponseCode::OK, Some(json_dag));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_job_graph(dag_metadata: Data<DagMetadata>) -> Result<HttpResponse, Error> {
    let json_dag = dag_metadata.get_ref().job_graph().clone();

    let response = StdResponse::new(ResponseCode::OK, Some(json_dag));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_execution_graph(
    dag_metadata: Data<DagMetadata>,
) -> Result<HttpResponse, Error> {
    let json_dag = dag_metadata.get_ref().execution_graph().clone();

    let response = StdResponse::new(ResponseCode::OK, Some(json_dag));
    Ok(HttpResponse::Ok().json(response))
}

// pub(crate) async fn get_physic_graph(dag_metadata: Data<DagMetadata>) -> Result<HttpResponse, Error> {
//     let task_groups = &dag_metadata.get_ref().physic_graph().task_groups.clone();
//
//     let response = StdResponse::new(ResponseCode::OK, Some(task_groups));
//     Ok(HttpResponse::Ok().json(response))
// }
