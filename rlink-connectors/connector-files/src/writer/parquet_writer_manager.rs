use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use parquet::file::properties::WriterPropertiesPtr;
use parquet::schema::types::TypePtr;
use rlink::channel::{bounded, Receiver, Sender};
use rlink::core::element::Record;
use rlink::core::runtime::TaskId;
use rlink::utils::date_time::current_timestamp;

use crate::writer::parquet_writer::{BlocksBuilder, ParquetBlockWriter};
use crate::writer::{BlockWriter, BlockWriterManager, FileSystem, FileSystemBuilder, PathLocation};

enum FlushData {
    Bytes((String, Vec<u8>)),
    Finish(Sender<bool>),
}

pub struct ParquetBlockWriterManager {
    row_group_size: usize,
    max_bytes: i64,
    schema: TypePtr,
    props: WriterPropertiesPtr,
    blocks_builder: Arc<Box<dyn BlocksBuilder>>,
    path_location: Box<dyn PathLocation>,

    path_writers: HashMap<String, (ParquetBlockWriter, Duration)>,

    bytes_flush_sender: Sender<FlushData>,
}

impl ParquetBlockWriterManager {
    pub fn new<FsB, FS, W>(
        row_group_size: usize,
        max_bytes: i64,
        schema: TypePtr,
        props: WriterPropertiesPtr,
        blocks_builder: Arc<Box<dyn BlocksBuilder>>,
        path_location: Box<dyn PathLocation>,
        ttl: Duration,
        fs_factory: FsB,
    ) -> Self
    where
        FsB: FileSystemBuilder<FS, W> + 'static,
        FS: FileSystem<W>,
        W: Write,
    {
        let (sender, receiver) = bounded(10);
        Self::fs_write(ttl, fs_factory, receiver);
        Self {
            row_group_size,
            max_bytes,
            schema,
            props,
            blocks_builder,
            path_location,
            path_writers: HashMap::new(),
            bytes_flush_sender: sender,
        }
    }

    fn fs_write<FsB, FS, W>(_ttl: Duration, fs_factory: FsB, bytes_receiver: Receiver<FlushData>)
    where
        FsB: FileSystemBuilder<FS, W> + 'static,
        FS: FileSystem<W>,
        W: Write,
    {
        rlink::utils::thread::spawn("file_writer", move || {
            let mut fs = fs_factory.build();

            while let Ok(data) = bytes_receiver.recv() {
                match data {
                    FlushData::Bytes((path, bytes)) => {
                        let mut writer = fs.create_write(path.as_str()).unwrap();
                        writer.write_all(bytes.as_slice()).unwrap();

                        info!("success write file {}", path);
                    }
                    FlushData::Finish(notify) => notify.send(true).unwrap(),
                }
            }
        });
    }

    fn create_writer(&self) -> ParquetBlockWriter {
        ParquetBlockWriter::new(
            self.row_group_size,
            self.max_bytes,
            self.schema.clone(),
            self.props.clone(),
            self.blocks_builder.clone(),
        )
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        let paths: Vec<String> = self.path_writers.keys().map(|x| x.clone()).collect();
        for path in paths {
            let (writer, _fs) = self.path_writers.remove(path.as_str()).unwrap();
            let bytes = writer.close()?;

            self.bytes_flush_sender
                .send(FlushData::Bytes((path, bytes)))
                .unwrap();
        }

        Ok(())
    }
}

impl BlockWriterManager for ParquetBlockWriterManager {
    fn open(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn append(&mut self, mut record: Record, task_id: &TaskId) -> anyhow::Result<()> {
        let path = self.path_location.path(record.borrow_mut(), task_id)?;

        let mut fs_writer = self.path_writers.get_mut(path.as_str());
        if fs_writer.is_none() {
            let writer_builder = self.create_writer();
            self.path_writers
                .insert(path.clone(), (writer_builder, current_timestamp()));

            fs_writer = self.path_writers.get_mut(path.as_str())
        }

        let (writer, _fs) = fs_writer.unwrap();
        let full = writer.append(record)?;
        if full {
            let (writer, _fs) = self.path_writers.remove(path.as_str()).unwrap();
            let bytes = writer.close()?;

            self.bytes_flush_sender
                .send(FlushData::Bytes((path, bytes)))
                .unwrap();
        }

        Ok(())
    }

    fn snapshot(&mut self) -> anyhow::Result<()> {
        self.flush()
    }

    fn close(&mut self) -> anyhow::Result<()> {
        self.flush()?;

        let (sender, receiver) = bounded(0);
        self.bytes_flush_sender
            .send(FlushData::Finish(sender))
            .unwrap();
        receiver.recv().unwrap();

        Ok(())
    }
}
