use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Once, RwLock};

use metrics::{counter, gauge};

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct Tag(pub(crate) String, pub(crate) String);

impl Tag {
    pub fn new<F, C>(field: F, context: C) -> Self
    where
        F: ToString,
        C: ToString,
    {
        Tag(field.to_string(), context.to_string())
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct KeyTags {
    name: String,
    tags: Vec<Tag>,
}

struct CounterMeta {
    key_tags: KeyTags,
    old_value: AtomicU64,
    value: Arc<AtomicU64>,
}

struct GaugeMeta {
    key_tags: KeyTags,
    value: Arc<AtomicI64>,
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

struct RecorderRaw {
    counters: HashMap<KeyTags, CounterMeta>,
    gauges: HashMap<KeyTags, GaugeMeta>,
}

impl RecorderRaw {
    fn new() -> Self {
        RecorderRaw {
            counters: HashMap::new(),
            gauges: HashMap::new(),
        }
    }

    fn register_counter<K>(&mut self, name: K, tags: Vec<Tag>) -> Counter
    where
        K: ToString,
    {
        let value = Arc::new(AtomicU64::new(0));

        let key_tags = KeyTags {
            name: name.to_string(),
            tags,
        };
        let meta = CounterMeta {
            key_tags: key_tags.clone(),
            old_value: AtomicU64::new(0),
            value: value.clone(),
        };

        self.counters.insert(key_tags, meta);

        Counter::new(value)
    }

    fn register_gauge<K>(&mut self, name: K, tags: Vec<Tag>) -> Gauge
    where
        K: ToString,
    {
        let value = Arc::new(AtomicI64::new(0));

        let key_tags = KeyTags {
            name: name.to_string(),
            tags,
        };
        let meta = GaugeMeta {
            key_tags: key_tags.clone(),
            value: value.clone(),
        };

        self.gauges.insert(key_tags, meta);

        Gauge::new(value)
    }

    pub fn counters(&self) -> Vec<(KeyTags, u64)> {
        self.counters
            .values()
            .map(|meta| {
                let value: u64 = meta.value.load(Ordering::Relaxed);
                let old_value: u64 = meta.old_value.load(Ordering::Relaxed);
                let incr = value - old_value;
                meta.old_value.store(value, Ordering::Relaxed);

                (meta.key_tags.clone(), incr)
            })
            .collect()
    }
    pub fn guavas(&self) -> Vec<(KeyTags, i64)> {
        self.gauges
            .values()
            .map(|meta| (meta.key_tags.clone(), meta.value.load(Ordering::Relaxed)))
            .collect()
    }
}

pub struct Recorder {
    raw: Arc<RwLock<RecorderRaw>>,
}
impl Recorder {
    pub fn new() -> Self {
        Recorder {
            raw: Arc::new(RwLock::new(RecorderRaw::new())),
        }
    }
    pub fn register_counter<K>(&self, name: K, tags: Vec<Tag>) -> Counter
    where
        K: ToString,
    {
        let mut guard = self.raw.write().unwrap();
        guard.register_counter(name, tags)
    }

    pub fn register_gauge<K>(&self, name: K, tags: Vec<Tag>) -> Gauge
    where
        K: ToString,
    {
        let mut guard = self.raw.write().unwrap();
        guard.register_gauge(name, tags)
    }

    pub fn export<T>(&self, mut exporter: T)
    where
        T: Exporter,
    {
        let guard = self.raw.write().unwrap();
        exporter.render_counters(guard.counters());
        exporter.render_gauges(guard.guavas());
    }
}

pub trait Exporter {
    fn render_counters(&mut self, counters: Vec<(KeyTags, u64)>);
    fn render_gauges(&mut self, guavas: Vec<(KeyTags, i64)>);
}

lazy_static! {
    static ref RECORDER: Recorder = Recorder::new();
    static ref MANAGER_ID: RwLock<Option<String>> = RwLock::new(None);
}

pub(crate) fn set_manager_id(manager_id: String) {
    let manager_id_rw: &RwLock<Option<String>> = &*MANAGER_ID;
    let mut n = manager_id_rw.write().unwrap();
    *n = Some(manager_id)
}

fn get_manager_id() -> String {
    let manager_id_rw: &RwLock<Option<String>> = &*MANAGER_ID;
    let n = manager_id_rw.read().unwrap();
    (*n).as_ref().unwrap().clone()
}

pub fn register_counter<K>(name: K, tags: Vec<Tag>) -> Counter
where
    K: ToString,
{
    let tags = {
        let mut t = vec![Tag::new("manager_id", get_manager_id())];
        t.extend_from_slice(tags.as_slice());
        t
    };

    let recorder: &Recorder = &*RECORDER;
    recorder.register_counter(name, tags)
}

pub fn register_gauge<K>(name: K, tags: Vec<Tag>) -> Gauge
where
    K: ToString,
{
    let tags = {
        let mut t = vec![Tag::new("manager_id", get_manager_id())];
        t.extend_from_slice(tags.as_slice());
        t
    };

    let recorder: &Recorder = &*RECORDER;
    recorder.register_gauge(name, tags)
}

struct MetricsExporter {}

impl Exporter for MetricsExporter {
    fn render_counters(&mut self, counters: Vec<(KeyTags, u64)>) {
        for (key_tags, value) in counters {
            let KeyTags { name, tags } = key_tags;

            let mut labels = Vec::new();
            for tag in tags {
                let Tag(field_name, field_value) = tag;
                labels.push((field_name, field_value));
            }
            counter!(name, value, &labels);
        }
    }

    fn render_gauges(&mut self, guavas: Vec<(KeyTags, i64)>) {
        for (key_tags, value) in guavas {
            let KeyTags { name, tags } = key_tags;

            let mut labels = Vec::new();
            for tag in tags {
                let Tag(field_name, field_value) = tag;
                labels.push((field_name, field_value));
            }
            gauge!(name, value as f64, &labels);
        }
    }
}

pub(crate) fn export() {
    static N: Once = Once::new();
    N.call_once(|| {
        crate::utils::thread::spawn("sysinfo", || {
            crate::utils::process::sys_info_metric_task(Tag::new("manager_id", get_manager_id()));
        });
    });

    let recorder: &Recorder = &*RECORDER;
    recorder.export(MetricsExporter {});
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
