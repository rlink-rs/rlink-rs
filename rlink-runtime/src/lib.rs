#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate async_trait;

#[macro_use]
mod macros;
mod cmd;
mod dag;
mod deployment;
mod env;
mod logger;
mod metrics;
mod runtime;
mod storage;
mod timer;
mod utils;

static mut ENV: Option<Box<env::RuntimeEnv>> = None;
static INIT: std::sync::Once = std::sync::Once::new();

pub fn environment() -> &'static Box<impl rlink_core::env::Env> {
    unsafe {
        INIT.call_once(|| {
            utils::panic::panic_notify();

            let env = Box::new(env::RuntimeEnv::new());
            ENV = Some(env);
        });
        ENV.as_ref().unwrap()
    }
}
