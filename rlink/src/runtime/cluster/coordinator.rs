use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::deployment::ResourceManager;
use crate::runtime::context::Context;
use crate::runtime::coordinator::CoordinatorTask;

pub(crate) fn run<S, R>(
    context: Context,
    stream_env: StreamExecutionEnvironment,
    stream_app: S,
    resource_manager: R,
) where
    S: StreamApp + 'static,
    R: ResourceManager + 'static,
{
    let mut coordinator_task =
        CoordinatorTask::new(context, stream_app, resource_manager, stream_env);
    coordinator_task.run();
}
