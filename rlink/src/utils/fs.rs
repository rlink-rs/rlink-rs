use std::fs::{DirBuilder, File};
use std::io::{Read, Write};
use std::path::PathBuf;

pub fn write_file(path: PathBuf, file_name: &str, values: &Vec<String>) -> std::io::Result<()> {
    DirBuilder::new().recursive(true).create(path.clone())?;

    let path = path.join(file_name);

    let mut output = File::create(path.clone())?;

    for value in values {
        writeln!(output, "{}", value.as_str())?;
    }

    Ok(())
}

pub fn read_file(path: &PathBuf) -> std::io::Result<String> {
    let mut file = File::open(path)?;

    let mut s = String::new();
    file.read_to_string(&mut s)?;

    Ok(s)
}
