use crate::api::env::{StreamExecutionEnvironment, StreamJob};
use crate::deployment::ResourceManagerWrap;
use crate::runtime::context::Context;
use crate::runtime::ManagerType;

mod coordinator;
mod worker;

pub(crate) fn run_task<S>(context: Context, stream_env: StreamExecutionEnvironment, stream_job: S)
where
    S: StreamJob + 'static,
{
    match context.manager_type {
        ManagerType::Coordinator => {
            let resource_manager = ResourceManagerWrap::new(&context);
            coordinator::run(context, stream_env, stream_job, resource_manager);
        }
        ManagerType::Standby => {}
        ManagerType::Worker => {
            worker::run(context, stream_env, stream_job);
        }
    };
}
