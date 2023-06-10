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
