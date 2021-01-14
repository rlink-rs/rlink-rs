use std::collections::HashMap;
use std::fs::{remove_file, DirBuilder};
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use actix_web::web::{Data, Path};
use actix_web::{web, Error, HttpResponse};
use rlink::api::cluster::{ExecuteRequest, ResponseCode, StdResponse};

use crate::config::Context;
use crate::utils::{current_timestamp_millis, read_file_as_lines};

lazy_static! {
    static ref TASK_MANAGER_START_TIME: u64 = current_timestamp_millis();
    static ref TASK_COUNTER: AtomicU64 = AtomicU64::new(0);
}

pub async fn execute_task(
    application_id: Path<String>,
    execute_model: web::Json<ExecuteRequest>,
    context: Data<Context>,
) -> Result<HttpResponse, Error> {
    let execute_model = execute_model.into_inner();

    let application_id = application_id.as_str();
    let application_manager_address = context.config.application_manager_address.clone();
    let executable_file = execute_model.executable_file.clone();
    let cluster_config = context.config_path.to_str().unwrap().to_string();

    let bind_ip = context.config.task_manager_bind_ip.clone();

    let task_manager_start_time: &u64 = &*TASK_MANAGER_START_TIME;
    let task_counter: &AtomicU64 = &*TASK_COUNTER;
    let task_id = format!(
        "{}-{}-{}",
        bind_ip,
        task_manager_start_time.clone(),
        task_counter.fetch_add(1 as u64, Ordering::SeqCst)
    );

    let script_path = context.script_path.clone().to_str().unwrap().to_string();
    let worker_path = PathBuf::from(context.config.task_manager_work_dir.as_str());
    let task_work_space = worker_path
        .clone()
        .join(application_id)
        .join(task_id.as_str());

    let mut envs = HashMap::new();
    envs.insert(
        "WORKER_PATH".to_string(),
        worker_path.to_str().unwrap().to_string(),
    );
    // TODO: JobManager `File Share` and load balance
    envs.insert(
        "APPLICATION_MANAGER_ADDRESS".to_string(),
        application_manager_address[0].clone(),
    );
    envs.insert("APPLICATION_ID".to_string(), application_id.to_string());
    envs.insert("TASK_ID".to_string(), task_id.clone());
    envs.insert("BIND_IP".to_string(), bind_ip);
    envs.insert("FILE_NAME".to_string(), executable_file);
    envs.insert("CLUSTER_CONFIG".to_string(), cluster_config);

    let execute_args: Vec<String> = execute_model
        .args
        .iter()
        .map(|(key, val)| format!("{}={}", key.as_str(), val.as_str()))
        .collect();

    DirBuilder::new()
        .recursive(true)
        .create(task_work_space.clone())
        .unwrap();

    execute_shell_command(execute_args, envs, script_path.as_str(), task_work_space);

    let response = StdResponse {
        code: ResponseCode::OK,
        data: Some(task_id),
    };
    Ok(HttpResponse::Ok().json(response))
}

fn execute_shell_command(
    args: Vec<String>,
    envs: HashMap<String, String>,
    script_path: &str,
    work_space: PathBuf,
) {
    info!(
        "execute command: {}, workspace: {}",
        script_path,
        work_space.to_str().unwrap()
    );

    Command::new("sh")
        .arg(script_path)
        .args(args)
        .envs(envs)
        .current_dir(work_space)
        .spawn()
        .unwrap();
}

#[derive(Deserialize)]
pub struct TaskModel {
    application_id: String,
    task_id: String,
}

pub async fn kill_task(
    task_model: Path<TaskModel>,
    context: Data<Context>,
) -> Result<HttpResponse, Error> {
    let pid_path = PathBuf::from(context.config.task_manager_work_dir.as_str())
        .join(task_model.application_id.as_str())
        .join(task_model.task_id.as_str())
        .join("pid");

    if !std::path::Path::new(pid_path.as_os_str()).exists() {
        let response = StdResponse {
            code: ResponseCode::OK,
            data: Some("pid file not found, maybe has killed".to_string()),
        };
        return Ok(HttpResponse::Ok().json(response));
    }

    let pid = read_file_as_lines(pid_path.clone())?;
    if pid.is_empty() || pid.len() > 1 {
        let response: StdResponse<String> = StdResponse {
            code: ResponseCode::ERR("pid context is illegal".to_string()),
            data: None,
        };
        return Ok(HttpResponse::Ok().json(response));
    }

    let pid = pid[0].as_str();
    info!("try invoke `kill -9 {}`", pid);
    Command::new("kill").arg("-9").arg(pid).spawn().unwrap();

    remove_file(pid_path)?;

    let response: StdResponse<String> = StdResponse {
        code: ResponseCode::OK,
        data: None,
    };
    Ok(HttpResponse::Ok().json(response))
}

pub async fn kill_job_tasks(
    application_id: Path<String>,
    context: Data<Context>,
) -> Result<HttpResponse, Error> {
    let app_path =
        PathBuf::from(context.config.task_manager_work_dir.as_str()).join(application_id.as_str());

    if !std::path::Path::new(app_path.as_os_str()).exists() {
        let response = StdResponse {
            code: ResponseCode::OK,
            data: Some("No application found".to_string()),
        };
        return Ok(HttpResponse::Ok().json(response));
    }

    let dirs: Vec<PathBuf> = std::fs::read_dir(app_path.clone())?
        .filter(|x| match x {
            Ok(p) => p.path().is_dir(),
            Err(e) => {
                error!("walk dir error. {}", e);
                false
            }
        })
        .map(|x| x.unwrap().path())
        .collect();

    info!(
        "begin walking path {}. children len {}",
        app_path.to_str().unwrap(),
        dirs.len()
    );

    for path in dirs {
        info!("try to read dir {}", path.to_str().unwrap());

        let pid_path = path.join("pid");
        if !std::path::Path::new(pid_path.as_os_str()).exists() {
            continue;
        }

        let pid = read_file_as_lines(pid_path.clone())?;
        if pid.is_empty() || pid.len() > 1 {
            let response: StdResponse<String> = StdResponse {
                code: ResponseCode::ERR("pid context is illegal".to_string()),
                data: None,
            };
            return Ok(HttpResponse::Ok().json(response));
        }

        let pid = pid[0].as_str();
        info!("try invoke `kill -9 {}`", pid);
        Command::new("kill").arg("-9").arg(pid).spawn().unwrap();

        remove_file(pid_path)?;
    }

    let response: StdResponse<String> = StdResponse {
        code: ResponseCode::OK,
        data: None,
    };
    Ok(HttpResponse::Ok().json(response))
}
