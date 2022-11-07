use metrics::{Counter, Gauge, Key, KeyName, Label};

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

pub fn register_counter<K>(name: K, tags: Vec<Tag>) -> Counter
where
    K: ToString,
{
    let tags: Vec<Label> = tags.into_iter().map(|t| Label::new(t.0, t.1)).collect();

    let key = Key::from_parts(KeyName::from(name.to_string()), tags);

    if let Some(recorder) = metrics::try_recorder() {
        recorder.register_counter(&key)
    } else {
        Counter::noop()
    }
}

pub fn register_gauge<K>(name: K, tags: Vec<Tag>) -> Gauge
where
    K: ToString,
{
    let tags: Vec<Label> = tags.into_iter().map(|t| Label::new(t.0, t.1)).collect();

    let key = Key::from_parts(KeyName::from(name.to_string()), tags);

    if let Some(recorder) = metrics::try_recorder() {
        recorder.register_gauge(&key)
    } else {
        Gauge::noop()
    }
}
