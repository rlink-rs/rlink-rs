use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::time::Duration;

use rlink::api::element::Record;
use rlink::channel::{bounded, Receiver, Sender};
use rlink::utils::date_time::current_timestamp;

use crate::writer::parquet_writer::ParquetBlockWriter;
use crate::writer::{BlockWriter, BlockWriterManager, FileSystemFactory, PathLocation};

pub struct ParquetBlockWriterManager<L>
where
    L: PathLocation,
{
    path_writers: HashMap<String, (ParquetBlockWriter, Duration)>,

    path_location: Option<L>,
    block_writer_builder: Option<Box<dyn Fn() -> ParquetBlockWriter>>,

    bytes_flush_sender: Sender<(String, Vec<u8>)>,
}

impl<L> ParquetBlockWriterManager<L>
where
    L: PathLocation,
{
    pub fn new<FSF>(ttl: Duration, fs_factory: FSF) -> Self
    where
        FSF: FileSystemFactory + 'static,
    {
        let (sender, receiver) = bounded(10);
        Self::fs_write(ttl, fs_factory, receiver);
        Self {
            path_writers: HashMap::new(),
            path_location: None,
            block_writer_builder: None,
            bytes_flush_sender: sender,
        }
    }

    fn fs_write<FSF>(_ttl: Duration, fs_factory: FSF, bytes_receiver: Receiver<(String, Vec<u8>)>)
    where
        FSF: FileSystemFactory + 'static,
    {
        rlink::utils::thread::spawn("", move || {
            let mut fs = fs_factory.build();

            while let Ok((path, bytes)) = bytes_receiver.recv() {
                fs.create_file(path.as_str()).unwrap();
                fs.write_all(bytes).unwrap();
                fs.close_file().unwrap();
            }
        });
    }
}

impl<L> BlockWriterManager<ParquetBlockWriter, L> for ParquetBlockWriterManager<L>
where
    L: PathLocation,
{
    fn open<F>(&mut self, path_location: L, block_writer_builder: F)
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

            self.path_writers
                .insert(path.clone(), (writer_builder(), current_timestamp()));

            fs_writer = self.path_writers.get_mut(path.as_str())
        }

        let (writer, _fs) = fs_writer.unwrap();
        let full = writer.append(record)?;
        if full {
            let (writer, fs) = self.path_writers.remove(path.as_str()).unwrap();
            let bytes = writer.close()?;

            self.bytes_flush_sender.send((path, bytes)).unwrap();
        }

        Ok(())
    }

    fn close(&mut self) {}
}
