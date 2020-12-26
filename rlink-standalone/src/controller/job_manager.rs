use crate::config::Context;
use crate::controller::get_resource_storage_path;
use crate::controller::HttpClientError;
use crate::job::{Job, Status};
use crate::utils::current_timestamp_millis;
use actix_multipart::Multipart;
use actix_web::http::StatusCode;
use actix_web::web::{Data, Path};
use actix_web::{web, Error, HttpRequest, HttpResponse};
use futures::{StreamExt, TryStreamExt};
use rlink::api::cluster::{
    BatchExecuteRequest, ExecuteRequest, ResponseCode, StdResponse, TaskResourceInfo,
};
use std::io::Write;

#[derive(Deserialize)]
pub struct ResourceRequestModel {
    application_id: String,
    file_name: String,
}

pub async fn download_application_resource(
    req_model: Path<ResourceRequestModel>,
    req: HttpRequest,
) -> Result<HttpResponse, Error> {
    let app_resource_path = get_resource_storage_path(req_model.application_id.as_str());
    let filepath = app_resource_path.join(req_model.file_name.as_str());
    actix_files::NamedFile::open(filepath)
        .unwrap()
        .set_status_code(StatusCode::from_u16(200).unwrap())
        .into_response(&req)
}

pub async fn create_application(
    mut payload: Multipart,
    _context: Data<Context>,
) -> Result<HttpResponse, Error> {
    let application_id = format!("application-{}", current_timestamp_millis());

    // iterate over multipart stream
    while let Ok(Some(mut field)) = payload.try_next().await {
        let content_type = field.content_disposition().unwrap();
        let filename = content_type.get_filename().unwrap();
        let storage_path = get_resource_storage_path(application_id.as_str());

        let filename = sanitize_filename::sanitize(&filename);
        let filepath = storage_path.join(filename.as_str());
        // File::create is blocking operation, use threadpool
        let mut f = web::block(|| std::fs::File::create(filepath))
            .await
            .unwrap();
        // Field in turn is stream of *Bytes* object
        while let Some(chunk) = field.next().await {
            let data = chunk.unwrap();
            // filesystem operations are blocking, we have to use threadpool
            f = web::block(move || f.write_all(&data).map(|_| f)).await?;
        }

        let job = Job::new(application_id.clone(), filename);
        job.storage(storage_path)?;
    }

    Ok(HttpResponse::Ok().json(application_id))
}

pub async fn submit_job(
    application_id: Path<String>,
    batch_execute_model: web::Json<BatchExecuteRequest>,
    context: Data<Context>,
) -> Result<HttpResponse, Error> {
    let application_id = application_id.as_str();

    let storage_path = get_resource_storage_path(application_id);
    let job = Job::load(storage_path.clone())?;
    if job.status == Status::Killed {
        let response: StdResponse<String> = StdResponse {
            code: ResponseCode::ERR("Job has killed".to_string()),
            data: None,
        };
        return Ok(HttpResponse::Ok().json(response));
    }

    let batch_execute_model = batch_execute_model.into_inner();
    // let executable_file = batch_execute_model.executable_file.as_str();
    let execute_models: Vec<ExecuteRequest> = batch_execute_model
        .batch_args
        .iter()
        .map(|args| ExecuteRequest {
            executable_file: job.execute_file.clone(),
            args: args.clone(),
        })
        .collect();

    if context.task_managers.len() == 0 {
        return Ok(HttpResponse::Ok().json("No TaskManager"));
    }

    let mut task_ids = Vec::new();
    let mut start_index = 0;
    for execute_model in &execute_models {
        let (task_result_info, index) = publish_task_0(
            application_id,
            execute_model,
            start_index,
            &context.task_managers,
        )
        .await?;

        start_index = if index == context.task_managers.len() {
            0
        } else {
            index + 1
        };

        task_ids.push(task_result_info);
    }

    let response_json = serde_json::to_string(&task_ids).unwrap();
    info!("response task resource infos: {}", &response_json);

    let response = StdResponse {
        code: ResponseCode::OK,
        data: Some(response_json),
    };
    Ok(HttpResponse::Ok().json(response))
}

async fn publish_task_0(
    application_id: &str,
    execute_model: &ExecuteRequest,
    start_index: usize,
    task_managers: &Vec<String>,
) -> Result<(TaskResourceInfo, usize), HttpClientError> {
    let mut indies = Vec::new();
    for n in start_index..task_managers.len() {
        indies.push(n);
    }
    for n in 0..start_index {
        indies.push(n);
    }

    for index in indies {
        let task_manager_id = execute_model
            .args
            .get("task_manager_id")
            .map(|x| x.clone())
            .unwrap_or("".to_string());
        info!("task_manager_id = {}", task_manager_id);

        let task_manager_address = task_managers[index].as_str();
        match publish_task(application_id, execute_model, task_manager_address).await {
            Ok(task_id) => {
                info!(
                    "publish task success on {} and response `task_id`={}",
                    task_manager_address,
                    task_id.as_str()
                );

                let task_resource_info = TaskResourceInfo::new(
                    task_id.clone(),
                    task_manager_address.to_string(),
                    task_manager_id,
                );
                return Ok((task_resource_info, index));
            }
            Err(e) => {
                error!("try schedule to {} failure. {}", task_manager_address, e);
                continue;
            }
        }
    }

    Err(HttpClientError::MessageStatusError(
        "No available TaskManager".to_string(),
    ))
}

async fn publish_task(
    application_id: &str,
    execute_model: &ExecuteRequest,
    task_manager_address: &str,
) -> Result<String, HttpClientError> {
    let url = format!(
        "http://{}:8371/task/{}",
        task_manager_address, application_id
    );
    let mut response = actix_web::client::Client::default()
        .post(url.as_str())
        .header("Accept", "application/json")
        .header("Content-type", "application/json")
        .send_json(execute_model)
        .await
        .map_err(|e| HttpClientError::from(e))?;

    let result_model = response
        .json::<StdResponse<String>>()
        .await
        .map_err(|e| HttpClientError::from(e))?;
    match result_model.code {
        ResponseCode::OK => Ok(result_model.data.unwrap_or("".to_string())),
        ResponseCode::ERR(msg) => Err(HttpClientError::from(msg)),
    }
}

pub async fn shutdown_tasks(
    application_id: Path<String>,
    task_ids: web::Json<Vec<TaskResourceInfo>>,
    _context: Data<Context>,
) -> Result<HttpResponse, Error> {
    let application_id = application_id.as_str();

    for n in &task_ids.into_inner() {
        let kill_result = kill_task(
            application_id,
            n.get_task_id(),
            n.get_task_manager_address(),
        )
        .await;
        match kill_result {
            Ok(msg) => info!(
                "kill task_id={} from {} success. {}",
                n.get_task_id(),
                n.get_task_manager_address(),
                msg
            ),
            Err(e) => error!(
                "kill task_id={} from {} failure. {}",
                n.get_task_id(),
                n.get_task_manager_address(),
                e
            ),
        }
    }

    let response: StdResponse<String> = StdResponse {
        code: ResponseCode::OK,
        data: None,
    };
    Ok(HttpResponse::Ok().json(response))
}

async fn kill_task(
    application_id: &str,
    task_id: &str,
    task_manager_address: &str,
) -> Result<String, HttpClientError> {
    let url = format!(
        "http://{}:8371/task/{}/{}/shutdown",
        task_manager_address, application_id, task_id
    );
    let mut response = actix_web::client::Client::default()
        .delete(url.as_str())
        .header("Accept", "application/json")
        .header("Content-type", "application/json")
        .send()
        .await
        .map_err(|e| HttpClientError::from(e))?;

    let result_model = response
        .json::<StdResponse<String>>()
        .await
        .map_err(|e| HttpClientError::from(e))?;
    match result_model.code {
        ResponseCode::OK => Ok(result_model.data.unwrap_or("".to_string())),
        ResponseCode::ERR(msg) => Err(HttpClientError::from(msg)),
    }
}

pub async fn kill_job(
    application_id: Path<String>,
    context: Data<Context>,
) -> Result<HttpResponse, Error> {
    let storage_path = get_resource_storage_path(application_id.as_str());
    let mut job = Job::load(storage_path.clone())?;
    job.status = Status::Killed;

    job.storage(storage_path)?;

    for task_manager in &context.task_managers {
        kill_job_task(application_id.as_str(), task_manager.as_str()).await?;
    }

    let response: StdResponse<String> = StdResponse {
        code: ResponseCode::OK,
        data: None,
    };
    Ok(HttpResponse::Ok().json(response))
}

async fn kill_job_task(
    application_id: &str,
    task_manager_address: &str,
) -> Result<String, HttpClientError> {
    let url = format!(
        "http://{}:8371/task/{}/shutdown",
        task_manager_address, application_id
    );
    let mut response = actix_web::client::Client::default()
        .delete(url.as_str())
        .header("Accept", "application/json")
        .header("Content-type", "application/json")
        .send()
        .await
        .map_err(|e| HttpClientError::from(e))?;

    let result_model = response
        .json::<StdResponse<String>>()
        .await
        .map_err(|e| HttpClientError::from(e))?;
    match result_model.code {
        ResponseCode::OK => Ok(result_model.data.unwrap_or("".to_string())),
        ResponseCode::ERR(msg) => Err(HttpClientError::from(msg)),
    }
}
