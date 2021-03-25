use std::sync::atomic::{AtomicUsize, Ordering};

pub fn spawn<F, T>(name: &str, f: F) -> std::thread::JoinHandle<T>
where
    F: FnOnce() -> T,
    F: Send + 'static,
    T: Send + 'static,
{
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(f)
        .expect("failed to spawn thread")
}

pub fn async_runtime(thread_name: &'static str) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name_fn(move || {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("A-{}-{}", thread_name, id)
        })
        .build()
        .unwrap()
}

pub fn async_runtime_multi(thread_name: &'static str, threads: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name_fn(move || {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            format!("A-{}-{}", thread_name, id)
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
