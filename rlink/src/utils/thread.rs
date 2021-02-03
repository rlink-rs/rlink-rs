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

pub fn get_runtime() -> tokio::runtime::Runtime {
    // tokio::runtime::Builder::new()
    //     .threaded_scheduler()
    //     .core_threads(6)
    //     .max_threads(6)
    //     .enable_all()
    //     .build()
    //     .unwrap()
    tokio::runtime::Runtime::new().unwrap()
}
