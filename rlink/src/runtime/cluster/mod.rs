use std::sync::Arc;

use crate::api::env::{StreamApp, StreamExecutionEnvironment};
use crate::deployment::ResourceManager;
use crate::runtime::context::Context;
use crate::runtime::ManagerType;

mod coordinator;
mod worker;

pub(crate) fn run_task<S>(
    context: Arc<Context>,
    stream_env: StreamExecutionEnvironment,
    stream_app: S,
) -> anyhow::Result<()>
where
    S: StreamApp + 'static,
{
    info!("num of cpu cores: {}", num_cpus::get());
    match context.manager_type {
        ManagerType::Coordinator => {
            let resource_manager = ResourceManager::new(context.clone());
            coordinator::run(context, stream_env, stream_app, resource_manager)
        }
        ManagerType::Worker => worker::run(context, stream_env, stream_app),
    }
}

// pub(crate) fn run_worker<S>(
//     context: Arc<Context>,
//     stream_env: StreamExecutionEnvironment,
//     stream_app: S,
// ) -> anyhow::Result<()> {
//     match context.cluster_mode {
//         ClusterMode::Local | ClusterMode::Standalone => {
//             worker::run(context, stream_env, stream_app)
//         }
//         ClusterMode::YARN => {
//             use std::time::Duration;
//
//             use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
//             use nix::unistd::{fork, ForkResult};
//             use nix::Error;
//
//             match unsafe { fork() } {
//                 Ok(ForkResult::Parent { child, .. }) => {
//                     println!(
//                         "Continuing execution in parent process, new child has pid: {}",
//                         child
//                     );
//
//                     loop {
//                         std::thread::sleep(Duration::from_secs(5));
//                         match waitpid(child, Some(WaitPidFlag::WNOHANG)) {
//                             Ok(WaitStatus::StillAlive) => {}
//                             Ok(WaitStatus::Exited(pid, status)) => {
//                                 info!("child process(pid={}) has exit. status({})", pid, status);
//                                 break;
//                             }
//                             Ok(WaitStatus::Signaled(pid, signal, _core_dump)) => {
//                                 info!(
//                                     "child process(pid={}) was killed by the given signal({})",
//                                     pid, signal
//                                 );
//                             }
//                             Ok(WaitStatus::Stopped(pid, signal)) => {
//                                 info!("child process(pid={}) is alive, but was stopped by the given signal({})", pid, signal);
//                             }
//                             Ok(WaitStatus::Continued(pid)) => {
//                                 info!("child process(pid={}) has resumed execution", pid);
//                             }
//                             Err(e) => {
//                                 error!("not found process pid={}, {}", child, e);
//                             }
//                             _ => {
//                                 warn!("unknown status");
//                             }
//                         }
//                         println!("{:?}", n);
//                     }
//                 }
//                 Ok(ForkResult::Child) => {
//                     info!("worker child process");
//                     worker::run(context, stream_env, stream_app)
//                 }
//                 Err(e) => {
//                     println!("Fork failed");
//                     Err(anyhow!(e))
//                 }
//             }
//         }
//     }
// }
