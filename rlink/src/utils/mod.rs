use std::collections::HashMap;
use std::fs::File;
use std::hash::Hash;
use std::io::Read;
use std::path::PathBuf;
use std::thread::JoinHandle;

// pub mod buffer;
pub mod date_time;
pub mod fs;
pub mod handover;
pub mod hash;
pub mod http_client;
pub mod id_generator;
pub mod ip;
pub mod panic;
pub mod timer;

pub use id_generator::gen_id;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");

lazy_static! {
    pub static ref EMPTY_SLICE: &'static [u8] = &[];
    pub static ref EMPTY_VEC: Vec<u8> = Vec::with_capacity(0);
}

pub fn hash_map_copy<K, V>(src: &HashMap<K, V>, dest: &mut HashMap<K, V>)
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    for (key, val) in src {
        dest.insert(key.clone(), val.clone());
    }
}

pub fn get_work_space() -> PathBuf {
    std::env::current_dir().expect("Get current dir error")
}

pub fn parse_arg_with(arg_key: &str, default_value: &str) -> String {
    parse_arg(arg_key).unwrap_or(default_value.to_string())
}

pub fn parse_arg(arg_key: &str) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    for arg in args.iter() {
        let a: String = arg.to_string();
        let tokens: Vec<&str> = a.split("=").collect();
        if tokens.len() != 2 {
            continue;
        }

        let key = tokens.get(0).expect("");
        if key.to_string().eq(arg_key) {
            let value = tokens.get(1).expect("");
            return Option::Some(value.to_string());
        }
    }

    return Option::None;
}

pub fn read_config_from_path(path: &str) -> Result<String, std::io::Error> {
    let mut file = File::open(path)?;
    let mut buffer = String::new();
    match file.read_to_string(&mut buffer) {
        Ok(_) => Ok(buffer),
        Err(e) => Err(e),
    }
}

pub fn spawn<F, T>(name: &str, f: F) -> JoinHandle<T>
where
    F: FnOnce() -> T,
    F: Send + 'static,
    T: Send + 'static,
{
    std::thread::Builder::new()
        .name(format!("S-{}", name))
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
