use std::borrow::BorrowMut;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn get_work_space() -> PathBuf {
    std::env::current_dir().expect("Get current dir error")
}

pub fn parse_arg(arg_key: String) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    for arg in args.iter() {
        let a: String = arg.to_string();
        let tokens: Vec<&str> = a.split("=").collect();
        if tokens.len() != 2 {
            continue;
        }

        let key = tokens.get(0).expect("");
        if key.to_string().eq(arg_key.as_str()) {
            let value = tokens.get(1).expect("");
            return Option::Some(value.to_string());
        }
    }

    return Option::None;
}

pub fn read_config_from_path(path: PathBuf) -> Result<String, std::io::Error> {
    let mut file = File::open(path)?;
    let mut buffer = String::new();
    match file.read_to_string(&mut buffer) {
        Ok(_) => Ok(buffer),
        Err(e) => Err(e),
    }
}

pub fn current_timestamp() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
}

pub fn current_timestamp_millis() -> u64 {
    current_timestamp().as_millis() as u64
}

pub fn read_file_as_lines(path: PathBuf) -> std::io::Result<Vec<String>> {
    let file = File::open(path)?;
    let fin = BufReader::new(file);

    let mut lines = Vec::new();
    for line in fin.lines() {
        let line = line?;
        if !line.is_empty() {
            lines.push(line);
        }
    }

    Ok(lines)
}

pub fn read_file_as_string(path: PathBuf) -> std::io::Result<String> {
    let file = File::open(path)?;
    let mut fin = BufReader::new(file);

    let mut s = String::new();
    fin.read_to_string(s.borrow_mut())?;

    Ok(s)
}
