use std::io::Write;
use std::sync::Arc;

use parquet::schema::parser::parse_message_type;
use parquet::schema::types::TypePtr;
use rlink::api::element::Record;
use rlink::api::runtime::TaskId;

pub mod file_system;
pub mod parquet_writer;
pub mod parquet_writer_manager;

pub trait FileSystem<W>
where
    W: Write,
{
    fn create_write(&mut self, path: &str) -> anyhow::Result<W>;
}

pub trait FileSystemBuilder<FS, W>
where
    Self: Send + Sync,
    FS: FileSystem<W>,
    W: Write,
{
    fn build(&self) -> FS;
}

pub trait PathLocation {
    fn path(&mut self, record: &mut Record, task_id: &TaskId) -> anyhow::Result<String>;
}

pub trait BlockWriter {
    fn open(&mut self);
    fn append(&mut self, record: Record) -> anyhow::Result<bool>;
    fn close(self) -> anyhow::Result<Vec<u8>>;
}

pub trait BlockWriterManager {
    fn open(&mut self) -> anyhow::Result<()>;
    fn append(&mut self, record: Record, task_id: &TaskId) -> anyhow::Result<()>;
    fn snapshot(&mut self) -> anyhow::Result<()>;
    fn close(&mut self) -> anyhow::Result<()>;
}

pub fn parse_parquet_message_type(schema: &str) -> anyhow::Result<TypePtr> {
    match parse_message_type(schema) {
        Ok(t) => Ok(Arc::new(t)),
        Err(e) => Err(anyhow!(e)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use parquet::basic::Compression;
    use parquet::data_type::ByteArray;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::schema::parser::parse_message_type;
    use rlink::api::element::Record;
    use rlink::utils::date_time::current_timestamp;

    use crate::writer::file_system::LocalFileSystemBuilder;
    use crate::writer::parquet_writer::{
        Blocks, BlocksBuilder, ColumnValues, ParquetBlockWriter, RecordBlocksBuilder,
    };
    use crate::writer::parquet_writer_manager::ParquetBlockWriterManager;
    use crate::writer::{BlockWriter, BlockWriterManager, PathLocation};
    use rlink::api::runtime::TaskId;

    const SCHEMA_STR: &'static str = r#"
message Document {
    required int32 DocId;
    required binary Context (UTF8);
}"#;

    struct TestBlocks {
        col0: Vec<i32>,
        col1: Vec<ByteArray>,
    }

    impl TestBlocks {
        pub fn with_capacity(capacity: usize) -> Self {
            TestBlocks {
                col0: Vec::with_capacity(capacity),
                col1: Vec::with_capacity(capacity),
            }
        }
    }

    impl From<(usize, Vec<u8>)> for TestBlocks {
        fn from((batch_size, _schema): (usize, Vec<u8>)) -> Self {
            TestBlocks::with_capacity(batch_size)
        }
    }

    impl Blocks for TestBlocks {
        fn append(&mut self, _record: Record) -> usize {
            self.col0.push(1);
            self.col1.push(ByteArray::from("0123456789"));

            self.col0.len()
        }

        fn flush(&mut self) -> Vec<ColumnValues> {
            let mut row_values = Vec::new();
            row_values.push(ColumnValues::Int32Values(self.col0.as_slice()));
            row_values.push(ColumnValues::ByteArrayValues(self.col1.as_slice()));
            row_values
        }
    }

    pub struct TestPathLocation {
        test_file: String,
        index: usize,
    }

    impl TestPathLocation {
        pub fn new() -> Self {
            let dir = std::env::temp_dir();
            let test_file = dir.as_path().join("test-path-location.parquet");
            let test_file = test_file.as_path().to_str().unwrap().to_string();

            println!("location path: {}", test_file);
            TestPathLocation {
                test_file,
                index: 0,
            }
        }
    }

    impl PathLocation for TestPathLocation {
        fn path(&mut self, _record: &mut Record, _task_id: &TaskId) -> anyhow::Result<String> {
            let p = format!("{}.{}", self.test_file.clone(), self.index);

            self.index += 1;
            if self.index >= 10 {
                self.index = 0;
            }

            Ok(p)
        }
    }

    #[test]
    pub fn writer_test() {
        let schema = Arc::new(parse_message_type(SCHEMA_STR).unwrap());
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .build(),
        );
        let blocks_builder = {
            let blocks = RecordBlocksBuilder::<TestBlocks>::new(Vec::new().as_slice());
            let blocks_builder: Box<dyn BlocksBuilder> = Box::new(blocks);
            Arc::new(blocks_builder)
        };

        let mut writer =
            ParquetBlockWriter::new(10000 * 10, 1024 * 1024, schema, props, blocks_builder);

        let begin = current_timestamp();

        let loops = 10000 * 1000;
        for _ in 0..loops {
            let full = writer.append(Record::with_capacity(1)).unwrap();
            if full {
                break;
            }
        }
        let bytes = writer.close();

        let end = current_timestamp();

        println!(
            "len: {}, loops: {}, ts: {}",
            bytes.as_ref().unwrap().len(),
            loops,
            end.checked_sub(begin).unwrap().as_millis()
        );
    }

    #[test]
    pub fn writer_manager_test() {
        let task_id = TaskId::default();
        let fs_builder = LocalFileSystemBuilder {};
        let path_location = Box::new(TestPathLocation::new());
        let schema = Arc::new(parse_message_type(SCHEMA_STR).unwrap());
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .build(),
        );
        let blocks_builder = {
            let blocks = RecordBlocksBuilder::<TestBlocks>::new(Vec::new().as_slice());
            let block_converter: Box<dyn BlocksBuilder> = Box::new(blocks);
            Arc::new(block_converter)
        };

        let mut manager = ParquetBlockWriterManager::new(
            10000 * 10,
            1024 * 1024,
            schema,
            props,
            blocks_builder,
            path_location,
            Duration::from_secs(10),
            fs_builder,
        );
        manager.open().unwrap();

        let begin = current_timestamp();
        let loops = 10000 * 10;
        for _ in 0..loops {
            manager.append(Record::with_capacity(1), &task_id).unwrap();
        }
        manager.close().unwrap();

        let end = current_timestamp();

        println!(
            "loops: {}, ts: {}",
            loops,
            end.checked_sub(begin).unwrap().as_millis()
        );
    }
}
