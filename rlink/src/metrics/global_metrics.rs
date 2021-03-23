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

//use std::collections::hash_map::Iter;
// use std::collections::HashMap;
//
// pub struct SafeValue {
//     v: AtomicI64,
// }
//
// impl SafeValue {
//     pub fn new() -> Self {
//         SafeValue {
//             v: AtomicI64::new(0),
//         }
//     }
//
//     #[inline]
//     pub fn store(&self, v: i64) {
//         self.v.store(v, std::sync::atomic::Ordering::Relaxed);
//     }
//
//     #[inline]
//     pub fn incr(&self, v: i64) {
//         self.v.fetch_add(v, std::sync::atomic::Ordering::Relaxed);
//     }
//
//     #[inline]
//     pub fn load(&self) -> i64 {
//         self.v.load(std::sync::atomic::Ordering::Relaxed)
//     }
// }
//
// pub struct UnSafeValue {
//     v: i64,
// }
//
// impl UnSafeValue {
//     pub fn new() -> Self {
//         UnSafeValue { v: 0 }
//     }
//
//     #[inline]
//     fn as_ptr(&self) -> *mut Self {
//         self as *const UnSafeValue as *mut UnSafeValue
//     }
//
//     #[inline]
//     pub fn store(&self, v: i64) {
//         let ptr = self.as_ptr();
//         unsafe {
//             (*ptr).v = v;
//         }
//     }
//
//     #[inline]
//     pub fn incr(&self, v: i64) {
//         let ptr = self.as_ptr();
//         unsafe {
//             (*ptr).v = (*ptr).v + v;
//         }
//     }
//
//     #[inline]
//     pub fn load(&self) -> i64 {
//         self.v
//     }
// }
//
// pub struct Metric {
//     name: String,
//     tags: Vec<Tag>,
//     value: Arc<UnSafeValue>,
// }
//
// pub struct Recorder {
//     raw: Arc<RwLock<RecorderRaw>>,
// }
//
// impl Recorder {
//     pub fn new() -> Self {
//         Recorder {
//             raw: Arc::new(RwLock::new(RecorderRaw::new())),
//         }
//     }
//
//     pub fn register_counter(&mut self, name: &str, tags: Vec<Tag>) -> Arc<UnSafeValue> {
//         let mut guard = self.raw.write().unwrap();
//         guard.register_counter(name, tags)
//     }
//
//     pub fn register_guava(&mut self, name: &str, tags: Vec<Tag>) -> Arc<UnSafeValue> {
//         let mut guard = self.raw.write().unwrap();
//         guard.register_guava(name, tags)
//     }
//
//     pub fn export(&self, mut exporter: Box<dyn Exporter>) {
//         let guard = self.raw.write().unwrap();
//         exporter.render_counters(guard.counters());
//         exporter.render_guavas(guard.guavas());
//     }
// }
//
// pub struct RecorderRaw {
//     counter: HashMap<String, Vec<Metric>>,
//     guava: HashMap<String, Vec<Metric>>,
// }
//
// impl RecorderRaw {
//     pub fn new() -> Self {
//         RecorderRaw {
//             counter: HashMap::new(),
//             guava: HashMap::new(),
//         }
//     }
//
//     pub fn register_counter(&mut self, name: &str, tags: Vec<Tag>) -> Arc<UnSafeValue> {
//         let value = Arc::new(UnSafeValue::new());
//         let metric = Metric {
//             name: name.to_string(),
//             tags,
//             value: value.clone(),
//         };
//
//         self.counter
//             .entry(name.to_string())
//             .or_insert(Vec::new())
//             .push(metric);
//
//         value
//     }
//
//     pub fn register_guava(&mut self, name: &str, tags: Vec<Tag>) -> Arc<UnSafeValue> {
//         let value = Arc::new(UnSafeValue::new());
//         let metric = Metric {
//             name: name.to_string(),
//             tags,
//             value: value.clone(),
//         };
//
//         self.guava
//             .entry(name.to_string())
//             .or_insert(Vec::new())
//             .push(metric);
//
//         value
//     }
//
//     pub fn counters(&self) -> Iter<String, Vec<Metric>> {
//         self.counter.iter()
//     }
//
//     pub fn guavas(&self) -> Iter<String, Vec<Metric>> {
//         self.guava.iter()
//     }
// }
//
// pub trait Exporter {
//     fn render_counters(&mut self, counters: Iter<String, Vec<Metric>>);
//     fn render_guavas(&mut self, guavas: Iter<String, Vec<Metric>>);
// }
//
// #[cfg(test)]
// mod tests {
//     use crate::metrics::global_metrics::{SafeValue, UnSafeValue};
//     use crate::utils::date_time::current_timestamp;
//     use std::sync::Arc;
//
//     #[test]
//     pub fn value_test() {
//         let value = Arc::new(UnSafeValue::new());
//
//         let value_c = value.clone();
//         let join_handle = std::thread::spawn(move || {
//             value_c.incr(1);
//         });
//
//         join_handle.join().unwrap();
//
//         assert_eq!(value.load(), 1);
//     }
//
//     #[test]
//     pub fn value1_test() {
//         let loops = 10000000;
//         let value = SafeValue::new();
//         let begin = current_timestamp();
//         for n in 0..loops {
//             value.incr(n);
//         }
//         let end = current_timestamp();
//         println!("{}", end.checked_sub(begin).unwrap().as_nanos());
//
//         let value = UnSafeValue::new();
//         let begin = current_timestamp();
//         for n in 0..loops {
//             value.incr(n);
//         }
//         let end = current_timestamp();
//         println!("{}", end.checked_sub(begin).unwrap().as_nanos());
//
//         let mut _value = 0;
//         let begin = current_timestamp();
//         for n in 0..loops {
//             _value += n;
//         }
//         let end = current_timestamp();
//         println!("{}", end.checked_sub(begin).unwrap().as_nanos());
//     }
// }
