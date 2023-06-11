use actix_web::web::Data;
use actix_web::{middleware, web, App, HttpResponse, HttpServer};

use crate::config::{create_context, Context};
use crate::controller::job_manager::{
    create_application, download_application_resource, kill_job, shutdown_tasks, submit_job,
};
use crate::controller::task_manager::{execute_task, kill_job_tasks, kill_task};
use crate::utils::parse_arg;

pub async fn index() -> HttpResponse {
    let html = r#"<html>
        <head><title>Upload Test</title></head>
        <body>
            <h1>Streaming</h1>
        </body>
    </html>"#;

    HttpResponse::Ok().body(html)
}

enum RoleType {
    JobManager,
    TaskManager,
}

impl RoleType {
    pub fn from_arg() -> Self {
        let role_type = parse_arg("type".to_string()).expect("`type` not found");
        match role_type.as_str() {
            "JobManager" => RoleType::JobManager,
            "TaskManager" => RoleType::TaskManager,
            _ => panic!("unknown type {}", role_type.as_str()),
        }
    }
}

pub async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    // std::fs::create_dir_all("./tmp").unwrap();

    let context = create_context().unwrap();
    let data = Data::new(context);

    let role_type = RoleType::from_arg();
    match role_type {
        RoleType::JobManager => run_as_job_manager(data).await,
        RoleType::TaskManager => run_as_task_manager(data).await,
    }
}

async fn run_as_job_manager(data: Data<Context>) -> std::io::Result<()> {
    let ip = "0.0.0.0:8770";
    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .app_data(data.clone())
            .service(web::resource("/").route(web::get().to(index)))
            // .service(web::resource("/upload").route(web::post().to(upload_file)))
            // .service(web::resource("/download").route(web::post().to(download_file)))
            .service(web::resource("/job/application").route(web::post().to(create_application)))
            .service(
                web::resource("/job/application/{application_id}")
                    .route(web::post().to(submit_job)),
            )
            .service(
                web::resource("/job/resource/{application_id}/{file_name}")
                    .route(web::get().to(download_application_resource)),
            )
            .service(
                web::resource("/job/application/{application_id}/task/shutdown")
                    .route(web::post().to(shutdown_tasks)),
            )
            .service(
                web::resource("/job/application/{application_id}/shutdown")
                    .route(web::post().to(kill_job)),
            )
    })
    .bind(ip)?
    .run()
    .await
}

async fn run_as_task_manager(data: Data<Context>) -> std::io::Result<()> {
    let ip = "0.0.0.0:8771";
    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .app_data(data.clone())
            .service(web::resource("/").route(web::get().to(index)))
            .service(web::resource("/task/{application_id}").route(web::post().to(execute_task)))
            .service(
                web::resource("/task/{application_id}/{task_id}/shutdown")
                    .route(web::delete().to(kill_task)),
            )
            .service(
                web::resource("/task/{application_id}/shutdown")
                    .route(web::delete().to(kill_job_tasks)),
            )
    })
    .bind(ip)?
    .run()
    .await
}
