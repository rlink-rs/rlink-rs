use rlink::api::element::Record;

pub mod parquet_writer;
pub mod parquet_writer_manager;

pub trait FileSystem {
    fn create_file(&mut self, path: &str) -> anyhow::Result<()>;
    fn write_all(&mut self, bytes: Vec<u8>) -> anyhow::Result<()>;
    fn close_file(&mut self) -> anyhow::Result<()>;
}

pub trait PathLocation {
    fn path(&self, record: &mut Record) -> anyhow::Result<String>;
}

pub trait BlockWriter {
    fn open(&mut self);
    fn append(&mut self, record: Record) -> anyhow::Result<bool>;
    fn close(self) -> anyhow::Result<Vec<u8>>;
}

pub trait BlockWriterManager<T, L, FS>
where
    T: BlockWriter,
    L: PathLocation,
    FS: FileSystem,
{
    fn open<F>(&mut self, path_location: L, block_writer_builder: F, fs: FS)
    where
        F: Fn() -> T + 'static;
    fn append(&mut self, record: Record) -> anyhow::Result<()>;
    fn close(&mut self);
}
