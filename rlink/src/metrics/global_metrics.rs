use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::Arc;
use std::sync::RwLock;

use metrics::counter;
use metrics::gauge;

#[derive(Clone, Debug)]
pub struct Tag(pub String, pub String);

impl<F, C> From<(F, C)> for Tag
where
    F: ToString,
    C: ToString,
{
    fn from((field, context): (F, C)) -> Self {
        Tag(field.to_string(), context.to_string())
    }
}

pub(crate) struct MetricMeta {
    pub(crate) name: String,
    pub(crate) tags: Vec<Tag>,
    pub(crate) value: Arc<AtomicI64>,
}

pub(crate) struct CounterMeta {
    pub(crate) name: String,
    pub(crate) tags: Vec<Tag>,
    pub(crate) old_value: AtomicU64,
    pub(crate) value: Arc<AtomicU64>,
}

lazy_static! {
    pub(crate) static ref COUNTER: RwLock<Vec<CounterMeta>> = RwLock::new(Vec::new());
    pub(crate) static ref GAUGE: RwLock<Vec<MetricMeta>> = RwLock::new(Vec::new());
    pub(crate) static ref MANAGER_ID: RwLock<Option<String>> = RwLock::new(None);
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
        let value: u64 = meta.value.load(Ordering::Relaxed);
        let old_value: u64 = meta.old_value.load(Ordering::Relaxed);
        let incr = value - old_value;
        meta.old_value.store(value, Ordering::Relaxed);

        let len = meta.tags.len();
        match len {
            0 => {
                counter!(
                    meta.name.clone(),
                    incr,
                    manager_id_key.to_string() => manager_id.clone(),
                );
            }
            1 => {
                let Tag(tag_key0, tag_val0) = meta.tags[0].clone();
                counter!(
                    meta.name.clone(),
                    incr,
                    manager_id_key.to_string() => manager_id.clone(),
                    tag_key0 => tag_val0
                );
            }
            2 => {
                let Tag(tag_key0, tag_val0) = meta.tags[0].clone();
                let Tag(tag_key1, tag_val1) = meta.tags[1].clone();
                counter!(
                    meta.name.clone(),
                    incr,
                    manager_id_key.to_string() => manager_id.clone(),
                    tag_key0 => tag_val0,
                    tag_key1 => tag_val1
                );
            }
            3 => {
                let Tag(tag_key0, tag_val0) = meta.tags[0].clone();
                let Tag(tag_key1, tag_val1) = meta.tags[1].clone();
                let Tag(tag_key2, tag_val2) = meta.tags[2].clone();

                counter!(
                    meta.name.clone(),
                    incr,
                    manager_id_key.to_string() => manager_id.clone(),
                    tag_key0 => tag_val0,
                    tag_key1 => tag_val1,
                    tag_key2 => tag_val2
                );
            }
            _ => {}
        }
    }
}

pub(crate) fn compute_gauge() {
    let manager_id_key = "manager_id";
    let manager_id = get_manager_id();

    let metrics: &RwLock<Vec<MetricMeta>> = &*GAUGE;
    let metrics = metrics.read().unwrap();
    for meta in &*metrics {
        let len = meta.tags.len();
        match len {
            0 => {
                gauge!(
                    meta.name.clone(),
                    meta.value.load(Ordering::Relaxed) as i64,
                    manager_id_key.to_string() => manager_id.clone(),
                );
            }
            1 => {
                let Tag(tag_key0, tag_val0) = meta.tags[0].clone();

                gauge!(
                    meta.name.clone(),
                    meta.value.load(Ordering::Relaxed) as i64,
                    manager_id_key.to_string() => manager_id.clone(),
                    tag_key0 => tag_val0
                );
            }
            2 => {
                let Tag(tag_key0, tag_val0) = meta.tags[0].clone();
                let Tag(tag_key1, tag_val1) = meta.tags[1].clone();

                gauge!(
                    meta.name.clone(),
                    meta.value.load(Ordering::Relaxed) as i64,
                    manager_id_key.to_string() => manager_id.clone(),
                    tag_key0 => tag_val0,
                    tag_key1 => tag_val1
                );
            }
            3 => {
                let Tag(tag_key0, tag_val0) = meta.tags[0].clone();
                let Tag(tag_key1, tag_val1) = meta.tags[1].clone();
                let Tag(tag_key2, tag_val2) = meta.tags[2].clone();

                gauge!(
                    meta.name.clone(),
                    meta.value.load(Ordering::Relaxed) as i64,
                    manager_id_key.to_string() => manager_id.clone(),
                    tag_key0 => tag_val0,
                    tag_key1 => tag_val1,
                    tag_key2 => tag_val2
                );
            }
            _ => {}
        }
    }
}
