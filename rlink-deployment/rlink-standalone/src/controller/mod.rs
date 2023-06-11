use std::fmt::Debug;
use std::fs::DirBuilder;
use std::path::PathBuf;

use crate::utils::get_work_space;

pub mod job_manager;
pub mod task_manager;

fn get_resource_storage_path(application_id: &str) -> PathBuf {
    let p = get_work_space().join("download").join(application_id);
    DirBuilder::new().recursive(true).create(p.clone()).unwrap();
    p
}

#[derive(Debug)]
pub enum HttpClientError {
    MessageStatusError(String),
    SendRequestError(awc::error::SendRequestError),
    JsonPayloadError(awc::error::JsonPayloadError),
}

impl actix_web::ResponseError for HttpClientError {}

impl std::error::Error for HttpClientError {
    // fn source(&self) -> Option<&dyn Error> {
    //     match self {
    //         HttpClientError::MessageStatusError(err_msg) => None,
    //         HttpClientError::SendRequestError(err) => err,
    //         HttpClientError::JsonPayloadError(err) => err,
    //     }
    // }
}

impl std::fmt::Display for HttpClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpClientError::MessageStatusError(err) => write!(f, "{}", err),
            HttpClientError::SendRequestError(err) => write!(f, "{}", err),
            HttpClientError::JsonPayloadError(err) => write!(f, "{}", err),
        }
    }
}

impl From<String> for HttpClientError {
    fn from(err: String) -> Self {
        HttpClientError::MessageStatusError(err)
    }
}

impl From<awc::error::SendRequestError> for HttpClientError {
    fn from(err: awc::error::SendRequestError) -> Self {
        HttpClientError::SendRequestError(err)
    }
}

impl From<awc::error::JsonPayloadError> for HttpClientError {
    fn from(err: awc::error::JsonPayloadError) -> Self {
        HttpClientError::JsonPayloadError(err)
    }
}
