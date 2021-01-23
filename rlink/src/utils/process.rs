use std::path::PathBuf;

pub fn get_work_space() -> PathBuf {
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
