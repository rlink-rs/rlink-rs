use std::sync::atomic::{AtomicUsize, Ordering};

static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);

fn gen_thread_name(thread_name: &'static str) -> String {
    let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
    format!("A-{}-{}", thread_name, id)
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize)]
pub(crate) struct ThreadInfo {
    thread_id: String,
    thread_name: String,
}

impl ThreadInfo {
    pub fn current() -> Self {
        ThreadInfo {
            thread_id: format!("0x{:x}", thread_id::get()),
            thread_name: std::thread::current().name().unwrap_or("").to_string(),
        }
    }
}

lazy_static! {
    static ref THREAD_INFOS: dashmap::DashSet<ThreadInfo> = dashmap::DashSet::new();
}

pub(crate) fn set_thread_info(thread_info: ThreadInfo) {
    let thread_infos: &dashmap::DashSet<ThreadInfo> = &*THREAD_INFOS;
    thread_infos.insert(thread_info);
}

#[allow(dead_code)]
pub(crate) fn get_thread_infos() -> Vec<ThreadInfo> {
    let thread_infos: &dashmap::DashSet<ThreadInfo> = &*THREAD_INFOS;
    let mut ti: Vec<ThreadInfo> = thread_infos.iter().map(|x| x.key().clone()).collect();
    ti.sort_by(|x, y| x.thread_name.cmp(&y.thread_name));
    ti
}

pub fn spawn<F, T>(name: &str, f: F) -> std::thread::JoinHandle<T>
where
    F: FnOnce() -> T,
    F: Send + 'static,
    T: Send + 'static,
{
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(|| {
            set_thread_info(ThreadInfo::current());
            f()
        })
        .expect("failed to spawn thread")
}

pub fn async_runtime(thread_name: &'static str) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name_fn(move || gen_thread_name(thread_name))
        .on_thread_start(|| {
            set_thread_info(ThreadInfo::current());
        })
        .build()
        .unwrap()
}

pub fn async_runtime_multi(thread_name: &'static str, threads: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name_fn(move || gen_thread_name(thread_name))
        .on_thread_start(|| {
            set_thread_info(ThreadInfo::current());
        })
        .worker_threads(threads)
        .build()
        .unwrap()
}

pub fn async_runtime_single() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

pub fn async_spawn<T>(task: T) -> tokio::task::JoinHandle<T::Output>
where
    T: std::future::Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::spawn(task)
}

pub async fn async_sleep(duration: std::time::Duration) {
    tokio::time::sleep(duration).await;
}
