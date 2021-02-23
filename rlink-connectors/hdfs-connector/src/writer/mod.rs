use rlink::api::element::Record;

pub mod parquet_writer;

pub trait BlockWriter {
    fn open(&mut self);
    fn append(&mut self, record: Record) -> anyhow::Result<bool>;
    fn close(self) -> Option<Vec<u8>>;
}

pub trait BlockWriterManager<T>
where
    T: BlockWriter,
{
    fn start(&mut self);
    fn append(&mut self, record: Record);
}
