use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use rlink::api::element::Record;
use rlink::utils::date_time::current_timestamp;

use crate::writer::parquet_writer::ParquetBlockWriter;
use crate::writer::{BlockWriter, BlockWriterManager, FileSystem, PathLocation};

struct PathWriter<FS>
where
    FS: FileSystem,
{
    path: String,
    block_writer: ParquetBlockWriter,
    fs: Arc<FS>,
    create_timestamp: Duration,
}

pub struct ParquetBlockWriterManager<L, FS>
where
    L: PathLocation,
    FS: FileSystem,
{
    path_writers: HashMap<String, (ParquetBlockWriter, FS)>,
    path_ttl: Arc<DashMap<String, (Duration, AtomicBool)>>,

    path_location: Option<L>,
    block_writer_builder: Option<Box<dyn Fn() -> ParquetBlockWriter>>,
    fs_builder: Option<Box<dyn Fn() -> FS>>,
}

impl<L, FS> ParquetBlockWriterManager<L, FS>
where
    L: PathLocation,
    FS: FileSystem,
{
    pub fn new() -> Self {
        ParquetBlockWriterManager {
            path_writers: HashMap::new(),
            path_ttl: Arc::new(DashMap::new()),
            path_location: None,
            block_writer_builder: None,
            fs_builder: None,
        }
    }

    fn flush(&mut self, mut fs: FS, path: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
        fs.create_file(path)?;
        fs.write_all(bytes)?;
        fs.close_file()
    }

    fn ttl_check(ttl: Duration) {}
}

impl<L, FS> BlockWriterManager<ParquetBlockWriter, L, FS> for ParquetBlockWriterManager<L, FS>
where
    L: PathLocation,
    FS: FileSystem,
{
    fn open<F>(&mut self, path_location: L, block_writer_builder: F, fs: FS)
    where
        F: Fn() -> ParquetBlockWriter + 'static,
    {
        self.path_location = Some(path_location);
        self.block_writer_builder = Some(Box::new(block_writer_builder));
    }

    fn append(&mut self, mut record: Record) -> anyhow::Result<()> {
        let path = self
            .path_location
            .as_mut()
            .unwrap()
            .path(record.borrow_mut())?;

        let mut fs_writer = self.path_writers.get_mut(path.as_str());
        if fs_writer.is_none() {
            let writer_builder = self.block_writer_builder.as_ref().unwrap();
            let fs = self.fs_builder.as_ref().unwrap();

            self.path_writers
                .insert(path.clone(), (writer_builder(), fs()));
            self.path_ttl
                .insert(path.clone(), (current_timestamp(), AtomicBool::new(false)));

            fs_writer = self.path_writers.get_mut(path.as_str())
        }

        let (writer, _fs) = fs_writer.unwrap();
        let full = writer.append(record)?;
        if full {
            let (writer, fs) = self.path_writers.remove(path.as_str()).unwrap();
            let bytes = writer.close()?;
            self.flush(fs, path.as_str(), bytes)?;
        }

        Ok(())
    }

    fn close(&mut self) {
        unimplemented!()
    }
}
