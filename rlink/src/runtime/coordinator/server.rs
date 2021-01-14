use std::sync::{Arc, Mutex};

use actix_web::http::header;
use actix_web::web::Data;
use actix_web::{middleware, web, App, Error, HttpResponse, HttpServer};
use rand::prelude::*;

use crate::api::checkpoint::Checkpoint;
use crate::api::cluster::{ResponseCode, StdResponse};
use crate::api::metadata::MetadataStorageMode;
use crate::dag::{DagManager, JsonDag};
use crate::runtime::coordinator::checkpoint_manager::CheckpointManager;
use crate::runtime::TaskManagerStatus;
use crate::storage::metadata::MetadataStorage;
use crate::storage::metadata::MetadataStorageWrap;
use crate::utils::VERSION;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct WebContext {
    job_context: crate::runtime::context::Context,
    metadata_mode: MetadataStorageMode,
}

pub(crate) fn web_launch(
    context: crate::runtime::context::Context,
    metadata_mode: MetadataStorageMode,
    checkpoint_manager: CheckpointManager,
    dag_manager: DagManager,
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
                dag_manager,
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
    job_context: crate::runtime::context::Context,
    metadata_mode: MetadataStorageMode,
    address: Arc<Mutex<Option<String>>>,
    checkpoint_manager: CheckpointManager,
    dag_manager: DagManager,
) {
    actix_rt::System::new("Coordinator Web UI")
        .block_on(serve(
            job_context,
            metadata_mode,
            address,
            checkpoint_manager,
            dag_manager,
        ))
        .unwrap();
}

async fn serve(
    job_context: crate::runtime::context::Context,
    metadata_mode: MetadataStorageMode,
    rt_address: Arc<Mutex<Option<String>>>,
    checkpoint_manager: CheckpointManager,
    dag_manager: DagManager,
) -> std::io::Result<()> {
    let context = WebContext {
        job_context,
        metadata_mode,
    };

    let ip = context.job_context.bind_ip.clone();

    let mut rng = rand::thread_rng();
    for _ in 0..30 {
        let port = rng.gen_range(10000, 30000);
        let address = format!("{}:{}", ip.as_str(), port);

        let data = Data::new(context.clone());
        let data_ck_manager = Data::new(checkpoint_manager.clone());
        let dag_manager = Data::new(dag_manager.clone());
        let server = HttpServer::new(move || {
            App::new()
                .app_data(data.clone())
                .app_data(data_ck_manager.clone())
                .app_data(dag_manager.clone())
                .wrap(middleware::Logger::default())
                .wrap(middleware::DefaultHeaders::new().header("X-Version", VERSION))
                .service(
                    web::resource("/")
                        .wrap(
                            middleware::DefaultHeaders::new()
                                .header(header::CONTENT_TYPE, "text/html; charset=UTF-8"),
                        )
                        .route(web::get().to(index)),
                )
                .service(web::resource("/heartbeat").route(web::post().to(heartbeat)))
                .service(web::resource("/context").route(web::get().to(get_context)))
                .service(web::resource("/metadata").route(web::get().to(get_metadata)))
                .service(web::resource("/checkpoint").route(web::post().to(register_checkpoint)))
                .service(web::resource("/checkpoints").route(web::get().to(get_checkpoint)))
                .service(web::resource("/dag/stream_graph").route(web::get().to(get_stream_graph)))
                .service(web::resource("/dag/job_graph").route(web::get().to(get_job_graph)))
                .service(
                    web::resource("/dag/execution_graph").route(web::get().to(get_execution_graph)),
                )
                .service(web::resource("/dag/physic_graph").route(web::get().to(get_physic_graph)))
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
        <head><title>rlink UI</title></head>
        <body>
            <h1>rlink</h1>
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
    let metadata_storage = MetadataStorageWrap::new(&context.metadata_mode);

    if !heartbeat_model.status.eq("ok") {
        error!("heart beat status: {}", heartbeat_model.status.as_str());
    }

    metadata_storage
        .update_task_status(
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
    let response = StdResponse::new(ResponseCode::OK, Some(context.job_context.clone()));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_metadata(context: Data<WebContext>) -> Result<HttpResponse, Error> {
    let metadata_storage = MetadataStorageWrap::new(&context.metadata_mode);
    let job_descriptor = metadata_storage.read_job_descriptor().unwrap();

    let response = StdResponse::new(ResponseCode::OK, Some(job_descriptor));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn register_checkpoint(
    ck_model: web::Json<Checkpoint>,
    ck_manager: Data<CheckpointManager>,
) -> Result<HttpResponse, Error> {
    debug!(
        "<<<<<< register checkpoint to coordinator. {:?}",
        &ck_model.0
    );
    let resp = match ck_manager.get_ref().add(ck_model.0) {
        Ok(_) => "ok",
        Err(e) => {
            error!("register checkpoint error. {}", e);
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

pub(crate) async fn get_stream_graph(dag_manager: Data<DagManager>) -> Result<HttpResponse, Error> {
    let dag = &dag_manager.get_ref().stream_graph().dag;
    let json_dag = JsonDag::dag_json(dag);

    let response = StdResponse::new(ResponseCode::OK, Some(json_dag));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_job_graph(dag_manager: Data<DagManager>) -> Result<HttpResponse, Error> {
    let dag = &dag_manager.get_ref().job_graph().dag;
    let json_dag = JsonDag::dag_json(dag);

    let response = StdResponse::new(ResponseCode::OK, Some(json_dag));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_execution_graph(
    dag_manager: Data<DagManager>,
) -> Result<HttpResponse, Error> {
    let dag = &dag_manager.get_ref().execution_graph().dag;
    let json_dag = JsonDag::dag_json(dag);

    let response = StdResponse::new(ResponseCode::OK, Some(json_dag));
    Ok(HttpResponse::Ok().json(response))
}

pub(crate) async fn get_physic_graph(dag_manager: Data<DagManager>) -> Result<HttpResponse, Error> {
    let task_groups = &dag_manager.get_ref().physic_graph().task_groups.clone();

    let response = StdResponse::new(ResponseCode::OK, Some(task_groups));
    Ok(HttpResponse::Ok().json(response))
}
