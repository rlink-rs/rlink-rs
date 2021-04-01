use rand::Rng;
use rlink::utils::process::parse_arg;

pub struct Config {
    pub cluster_name: String,
    pub namespace: String,
    pub image_path: String,
    pub job_v_cores: usize,
    pub job_memory_mb: usize,
    pub num_task_managers: usize,
    pub task_v_cores: usize,
    pub task_memory_mb: usize,
}

impl Config {
    pub fn try_default() -> Self {
        let mut cfg = Config {
            cluster_name: String::new(),
            namespace: String::from("default"),
            image_path: String::new(),
            job_v_cores: 1,
            job_memory_mb: 100,
            num_task_managers: 1,
            task_v_cores: 1,
            task_memory_mb: 100,
        };

        match parse_arg("cluster_name") {
            Ok(o) => cfg.cluster_name = o,
            _ => cfg.cluster_name = format!("rlink-{}", rand_suffix()),
        }

        match parse_arg("image_path") {
            Ok(o) => cfg.image_path = o,
            _ => panic!("argument `image_path` is missing"),
        }

        match parse_arg("job_v_cores") {
            Ok(o) => cfg.job_v_cores = o.parse().expect("job_v_cores must a usize num"),
            _ => {}
        }

        match parse_arg("job_memory_mb") {
            Ok(o) => cfg.job_memory_mb = o.parse().expect("job_memory_mb must a usize num"),
            _ => {}
        }

        match parse_arg("task_v_cores") {
            Ok(o) => cfg.task_v_cores = o.parse().expect("task_v_cores must a usize num"),
            _ => {}
        }

        match parse_arg("task_memory_mb") {
            Ok(o) => cfg.task_memory_mb = o.parse().expect("task_memory_mb must a usize num"),
            _ => {}
        }

        match parse_arg("num_task_managers") {
            Ok(o) => cfg.num_task_managers = o.parse().expect("num_task_managers must a usize num"),
            _ => {}
        }
        cfg
    }
}

fn rand_suffix() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
    abcdefghijklmnopqrstuvwxyz";

    const SUFFIX_LEN: usize = 8;

    let mut rng = rand::thread_rng();

    let suffix: String = (0..SUFFIX_LEN)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    return suffix;
}
