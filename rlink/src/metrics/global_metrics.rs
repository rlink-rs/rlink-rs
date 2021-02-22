use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;
use std::sync::RwLock;

use metrics::{counter, gauge};

#[derive(Clone, Debug)]
pub struct Tag(String, String);

impl<F, C> From<(F, C)> for Tag
where
    F: ToString,
    C: ToString,
{
    fn from((field, context): (F, C)) -> Self {
        Tag(field.to_string(), context.to_string())
    }
}

struct MetricMeta {
    name: String,
    tags: Vec<Tag>,
    value: Arc<AtomicI64>,
}

struct CounterMeta {
    name: String,
    tags: Vec<Tag>,
    old_value: AtomicU64,
    value: Arc<AtomicU64>,
}

lazy_static! {
    static ref COUNTER: RwLock<Vec<CounterMeta>> = RwLock::new(Vec::new());
    static ref GAUGE: RwLock<Vec<MetricMeta>> = RwLock::new(Vec::new());
    static ref MANAGER_ID: RwLock<Option<String>> = RwLock::new(None);
}

pub(crate) fn set_manager_id(manager_id: &str, ip: &str) {
    let manager_id_rw: &RwLock<Option<String>> = &*MANAGER_ID;
    let mut n = manager_id_rw.write().unwrap();
    *n = Some(format!("{}-{}", manager_id, ip))
}

pub fn get_manager_id() -> String {
    let manager_id_rw: &RwLock<Option<String>> = &*MANAGER_ID;
    let n = manager_id_rw.read().unwrap();
    (*n).as_ref().unwrap().clone()
}

pub fn register_counter(name: &str, tags: Vec<Tag>, value: Arc<AtomicU64>) {
    let meta = CounterMeta {
        name: name.to_string(),
        tags,
        old_value: AtomicU64::new(0),
        value,
    };

    let metrics: &RwLock<Vec<CounterMeta>> = &*COUNTER;
    let mut n = metrics.write().unwrap();
    (*n).push(meta);

    // if metrics.contains_key(meta.name.as_str()) {
    //     error!("duplicate channel metrics key {}", meta.name.as_str());
    //     return;
    // }
    //
    // metrics.insert(meta.name.clone(), meta);
}

pub fn register_gauge(name: &str, tags: Vec<Tag>, value: Arc<AtomicI64>) {
    let meta = MetricMeta {
        name: name.to_string(),
        tags,
        value,
    };

    let metrics: &RwLock<Vec<MetricMeta>> = &*GAUGE;
    let mut n = metrics.write().unwrap();
    (*n).push(meta);

    // if metrics.contains_key(meta.name.as_str()) {
    //     error!("duplicate channel metrics key {}", meta.name.as_str());
    //     return;
    // }
    //
    // metrics.insert(meta.name.clone(), meta);
}

pub(crate) fn compute() {
    debug!("build metrics");
    compute_counter();
    compute_gauge();
}

pub(crate) fn compute_counter() {
    let manager_id_key = "manager_id";
    let manager_id = get_manager_id();

    let metrics: &RwLock<Vec<CounterMeta>> = &*COUNTER;
    let metrics = metrics.read().unwrap();
    for meta in &*metrics {
        let name = meta.name.clone();

        let value: u64 = meta.value.load(Ordering::Relaxed);
        let old_value: u64 = meta.old_value.load(Ordering::Relaxed);
        let incr = value - old_value;
        meta.old_value.store(value, Ordering::Relaxed);

        let mut labels = Vec::new();
        labels.push((manager_id_key.to_string(), manager_id.clone()));
        for tag in &meta.tags {
            labels.push((tag.0.clone(), tag.1.clone()));
        }

        counter!(name, incr, &labels);
    }
}

pub(crate) fn compute_gauge() {
    let manager_id_key = "manager_id";
    let manager_id = get_manager_id();

    let metrics: &RwLock<Vec<MetricMeta>> = &*GAUGE;
    let metrics = metrics.read().unwrap();
    for meta in &*metrics {
        let name = meta.name.clone();

        let mut labels = Vec::new();
        labels.push((manager_id_key.to_string(), manager_id.clone()));
        for tag in &meta.tags {
            labels.push((tag.0.clone(), tag.1.clone()));
        }

        let val = meta.value.load(Ordering::Relaxed) as f64;
        gauge!(name, val, &labels);
    }
}
