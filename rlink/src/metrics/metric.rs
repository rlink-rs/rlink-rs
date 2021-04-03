use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use metrics::{counter, gauge};

#[derive(Clone, Debug)]
pub struct Tag(String, String);

impl Tag {
    pub fn new<F, C>(field: F, context: C) -> Self
    where
        F: ToString,
        C: ToString,
    {
        Tag(field.to_string(), context.to_string())
    }
}

struct CounterMeta {
    name: String,
    tags: Vec<Tag>,
    old_value: AtomicU64,
    value: Arc<AtomicU64>,
}

struct GaugeMeta {
    name: String,
    tags: Vec<Tag>,
    value: Arc<AtomicI64>,
}

lazy_static! {
    static ref COUNTER: RwLock<Vec<CounterMeta>> = RwLock::new(Vec::new());
    static ref GAUGE: RwLock<Vec<GaugeMeta>> = RwLock::new(Vec::new());
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

#[derive(Clone, Default, Debug)]
pub struct Counter {
    value: Arc<AtomicU64>,
}

impl Counter {
    fn new(value: Arc<AtomicU64>) -> Self {
        Counter { value }
    }

    pub fn fetch_add(&self, v: u64) -> u64 {
        self.value.fetch_add(v, Ordering::Relaxed)
    }

    pub fn load(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Default, Debug)]
pub struct Gauge {
    value: Arc<AtomicI64>,
}

impl Gauge {
    fn new(value: Arc<AtomicI64>) -> Self {
        Gauge { value }
    }

    pub fn store(&self, v: i64) {
        self.value.store(v, Ordering::Relaxed);
    }

    pub fn fetch_add(&self, v: i64) {
        self.value.fetch_add(v, Ordering::Relaxed);
    }

    pub fn fetch_sub(&self, v: i64) {
        self.value.fetch_sub(v, Ordering::Relaxed);
    }

    pub fn load(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

pub fn register_counter<K>(name: K, tags: Vec<Tag>) -> Counter
where
    K: ToString,
{
    let value = Arc::new(AtomicU64::new(0));
    let meta = CounterMeta {
        name: name.to_string(),
        tags,
        old_value: AtomicU64::new(0),
        value: value.clone(),
    };

    let metrics: &RwLock<Vec<CounterMeta>> = &*COUNTER;
    let mut n = metrics.write().unwrap();
    (*n).push(meta);

    Counter::new(value)
}

pub fn register_gauge<K>(name: K, tags: Vec<Tag>) -> Gauge
where
    K: ToString,
{
    let value = Arc::new(AtomicI64::new(0));
    let meta = GaugeMeta {
        name: name.to_string(),
        tags,
        value: value.clone(),
    };

    let metrics: &RwLock<Vec<GaugeMeta>> = &*GAUGE;
    let mut n = metrics.write().unwrap();
    (*n).push(meta);

    Gauge::new(value)
}

pub(crate) fn compute() {
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

    let metrics: &RwLock<Vec<GaugeMeta>> = &*GAUGE;
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
