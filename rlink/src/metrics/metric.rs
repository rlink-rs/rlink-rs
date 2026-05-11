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
    let labels: Vec<(String, String)> = key
        .labels()
        .map(|l| (l.key().to_string(), l.value().to_string()))
        .collect();

    if labels.is_empty() {
        metrics::counter!(key.name().to_string())
    } else {
        metrics::counter!(key.name().to_string(), &labels)
    }
}

pub fn register_gauge<K>(name: K, tags: Vec<Tag>) -> Gauge
where
    K: ToString,
{
    let tags: Vec<Label> = tags.into_iter().map(|t| Label::new(t.0, t.1)).collect();
    let key = Key::from_parts(KeyName::from(name.to_string()), tags);
    let labels: Vec<(String, String)> = key
        .labels()
        .map(|l| (l.key().to_string(), l.value().to_string()))
        .collect();

    if labels.is_empty() {
        metrics::gauge!(key.name().to_string())
    } else {
        metrics::gauge!(key.name().to_string(), &labels)
    }
}
