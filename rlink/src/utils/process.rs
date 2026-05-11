use std::path::PathBuf;
use std::str::FromStr;

use metrics::gauge;
use sysinfo::{Pid, ProcessesToUpdate, System};

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

pub fn parse_arg_to_u64(arg_key: &str) -> anyhow::Result<u64> {
    let v = parse_arg(arg_key)?;
    u64::from_str(v.as_str()).map_err(|e| anyhow!(e))
}

pub(crate) fn sys_info_metric_task() {
    let mut system = System::new();
    let pid = Pid::from_u32(std::process::id());
    loop {
        system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
        system.refresh_memory();

        let load_avg = System::load_average();
        gauge!("sys_load_average", "minute" => "one").set(load_avg.one);
        gauge!("sys_load_average", "minute" => "five").set(load_avg.five);
        gauge!("sys_load_average", "minute" => "fifteen").set(load_avg.fifteen);

        if let Some(p) = system.process(pid) {
            gauge!("proc_cpu_usage").set(p.cpu_usage() as f64);
            gauge!("proc_memory").set(p.memory() as f64);
        }

        gauge!("sys_used_swap").set(system.used_swap() as f64);
        gauge!("sys_available_memory").set(system.available_memory() as f64);

        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}
