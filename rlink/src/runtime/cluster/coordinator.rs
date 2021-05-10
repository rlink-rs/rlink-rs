use std::sync::Arc;

use crate::core::env::{StreamApp, StreamExecutionEnvironment};
use crate::deployment::TResourceManager;
use crate::runtime::context::Context;
use crate::runtime::coordinator::CoordinatorTask;

pub(crate) fn run<S, R>(
    context: Arc<Context>,
    stream_env: StreamExecutionEnvironment,
    stream_app: S,
    resource_manager: R,
) -> anyhow::Result<()>
where
    S: StreamApp + 'static,
    R: TResourceManager + 'static,
{
    let mut coordinator_task =
        CoordinatorTask::new(context, stream_app, resource_manager, stream_env);
    coordinator_task.run()
}
