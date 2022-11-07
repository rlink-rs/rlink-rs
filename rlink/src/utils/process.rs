use std::path::PathBuf;
use std::str::FromStr;

use metrics::gauge;
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

pub fn parse_arg_to_u64(arg_key: &str) -> anyhow::Result<u64> {
    let v = parse_arg(arg_key)?;
    u64::from_str(v.as_str()).map_err(|e| anyhow!(e))
}

pub(crate) fn sys_info_metric_task() {
    let mut system = sysinfo::System::new();
    let pid = sysinfo::get_current_pid().unwrap();
    loop {
        system.refresh_process(pid);
        // system.refresh_cpu();
        system.refresh_memory();

        let load_avg = system.load_average();
        gauge!("sys_load_average", load_avg.one, "minute" => "one");
        gauge!("sys_load_average", load_avg.five, "minute" => "five");
        gauge!("sys_load_average", load_avg.fifteen, "minute" => "fifteen");

        if let Some(p) = system.process(pid) {
            gauge!("proc_cpu_usage", p.cpu_usage() as f64);
            gauge!("proc_memory", p.memory() as f64);
        }

        gauge!("sys_used_swap", system.used_swap() as f64);
        gauge!("sys_available_memory", system.available_memory() as f64);

        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}
