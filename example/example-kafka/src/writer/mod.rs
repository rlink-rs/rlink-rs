use std::io::{Result as IoResult, Write};
use uuid::Uuid;
use webhdfs::sync_client::WriteHdfsFile;
use webhdfs::*;

use rlink_connector_files::writer::{FileSystem, FileSystemBuilder};

pub struct HdfsWriter {
    url: String,
    user: String,
    partition_path: String,
}

impl HdfsWriter {
    fn new(url: String, user: String, partition_path: String) -> Self {
        let mut partition_path = partition_path;
        if partition_path.ends_with("/") {
            let n = partition_path.len();
            partition_path = partition_path[0..n - 1].to_string();
        }
        Self {
            url,
            user,
            partition_path,
        }
    }
}

impl Write for HdfsWriter {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let path = Uuid::new_v4();
        let path = format!("{}/{}.parquet", self.partition_path, path);
        let cx = SyncHdfsClientBuilder::new(self.url.parse().unwrap())
            .user_name(self.user.to_owned())
            .build()
            .unwrap();    
        let mut file =
            WriteHdfsFile::create(cx, path.clone(), CreateOptions::new(), AppendOptions::new())?;
        let size = match file.write(buf) {
            Ok(result) => {
                info!("write to hdfs success, path:{}", path.clone());
                result
            }
            Err(e) => {
                info!("write to hdfs failed:{}", e,);
                0
            }
        };
        Ok(size)
    }
    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

pub struct HdfsFileSystem {
    url: String,
    user: String,
}

impl HdfsFileSystem {
    pub fn new(url: String, user: String) -> Self {
        HdfsFileSystem { url, user }
    }
}

impl FileSystem<HdfsWriter> for HdfsFileSystem {
    fn create_write(&mut self, path: &str) -> anyhow::Result<HdfsWriter> {
        //todo check hdfs file path
        Ok(HdfsWriter::new(
            self.url.clone(),
            self.user.clone(),
            path.to_string(),
        ))
    }
}

pub struct HdfsFileSystemBuilder {
    pub url: String,
    pub user: String,
}

impl HdfsFileSystemBuilder {}

impl FileSystemBuilder<HdfsFileSystem, HdfsWriter> for HdfsFileSystemBuilder {
    fn build(&self) -> HdfsFileSystem {
        HdfsFileSystem::new(self.url.clone(), self.user.clone())
    }
}
