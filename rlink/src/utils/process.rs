use crate::metrics::Tag;
use metrics::gauge;
use std::path::PathBuf;
use sysinfo::{ProcessExt, SystemExt};

pub fn work_space() -> PathBuf {
    std::env::current_dir().expect("Get current dir error")
}

pub fn parse_arg_with(arg_key: &str, default_value: &str) -> String {
    parse_arg(arg_key).unwrap_or(default_value.to_string())
}

pub fn parse_arg(arg_key: &str) -> anyhow::Result<String> {
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
            return Ok(value.to_string());
        }
    }

    return Err(anyhow!("`{}` argument is not found", arg_key));
}

pub(crate) fn sys_info_metric_task(global_tag: Tag) {
    let mut labels = Vec::new();
    let Tag(field_name, field_value) = global_tag.clone();
    labels.push((field_name, field_value));

    let mut system = sysinfo::System::new();
    let pid = sysinfo::get_current_pid().unwrap();
    loop {
        system.refresh_process(pid);
        // system.refresh_cpu();
        system.refresh_memory();

        let load_avg = system.get_load_average();
        {
            let mut labels = labels.clone();
            labels.push(("minute".to_owned(), "one".to_owned()));
            gauge!("sys_load_average", load_avg.one, &labels,);
        }
        {
            let mut labels = labels.clone();
            labels.push(("minute".to_owned(), "five".to_owned()));
            gauge!("sys_load_average", load_avg.five, &labels,);
        }
        {
            let mut labels = labels.clone();
            labels.push(("minute".to_owned(), "fifteen".to_owned()));
            gauge!("sys_load_average", load_avg.fifteen, &labels,);
        }

        let cpu_usage = system
            .get_process(pid)
            .map(|p| p.cpu_usage())
            .unwrap_or_default();
        {
            let labels = labels.clone();
            gauge!("sys_cpu_usage", cpu_usage as f64, &labels,);
        }

        let used_memory = system.get_used_memory();
        {
            let mut labels = labels.clone();
            labels.push(("type".to_owned(), "used_memory".to_owned()));
            gauge!("sys_memory", used_memory as f64, &labels,);
        }
        let used_swap = system.get_used_swap();
        {
            let mut labels = labels.clone();
            labels.push(("type".to_owned(), "used_swap".to_owned()));
            gauge!("sys_memory", used_swap as f64, &labels,);
        }

        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}
