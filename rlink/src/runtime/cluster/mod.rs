use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::deployment::ResourceManager;
use crate::runtime::context::Context;
use crate::runtime::ManagerType;

mod coordinator;
mod worker;

pub(crate) fn run_task<S>(context: Context, stream_env: StreamExecutionEnvironment, stream_app: S)
where
    S: StreamApp + 'static,
{
    match context.manager_type {
        ManagerType::Coordinator => {
            let resource_manager = ResourceManager::new(&context);
            coordinator::run(context, stream_env, stream_app, resource_manager);
        }
        ManagerType::Standby => {}
        ManagerType::Worker => {
            worker::run(context, stream_env, stream_app);
        }
    };
}
