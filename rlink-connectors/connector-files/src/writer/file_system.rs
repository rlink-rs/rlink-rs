use std::fs::File;

use crate::writer::{FileSystem, FileSystemBuilder};

pub struct LocalFileSystem {}

impl FileSystem<File> for LocalFileSystem {
    fn create_write(&mut self, path: &str) -> anyhow::Result<File> {
        File::create(path).map_err(|e| anyhow!(e))
    }
}

pub struct LocalFileSystemBuilder {}

impl FileSystemBuilder<LocalFileSystem, File> for LocalFileSystemBuilder {
    fn build(&self) -> LocalFileSystem {
        LocalFileSystem {}
    }
}
