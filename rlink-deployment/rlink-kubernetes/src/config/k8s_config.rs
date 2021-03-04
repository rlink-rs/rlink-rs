pub struct JobConfig {
    pub namespace: String,
    pub image: String,
    pub cpu: f32,
    pub memory: String,
}

impl JobConfig {
    pub fn default() -> Self {
        JobConfig {
            namespace: String::from("default"),
            image: String::from("rlink"),
            cpu: 1.0,
            memory: String::from("200Mi"),
        }
    }
}
