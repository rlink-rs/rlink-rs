use std::sync::Arc;
use std::time::Duration;

use parquet::basic::Compression;
use parquet::data_type::ByteArray;
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::schema::parser::parse_message_type;
use rlink::api::element::Record;
use rlink::api::runtime::TaskId;
use rlink::api::window::TWindow;
use rlink::utils::date_time::fmt_date_time;
use rlink_files_connector::sink::output_format::HdfsOutputFormat;
use rlink_files_connector::writer::file_system::LocalFileSystemBuilder;
use rlink_files_connector::writer::parquet_writer::{
    Blocks, BlocksBuilder, ColumnValues, RecordBlocksBuilder,
};
use rlink_files_connector::writer::parquet_writer_manager::ParquetBlockWriterManager;
use rlink_files_connector::writer::PathLocation;

pub fn create_hdfs_sink(field_types: &[u8]) -> HdfsOutputFormat {
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
        let blocks = RecordBlocksBuilder::<TestBlocks>::new(field_types);
        let blocks_builder: Box<dyn BlocksBuilder> = Box::new(blocks);
        Arc::new(blocks_builder)
    };
    let writer_manager = ParquetBlockWriterManager::new(
        10000 * 10,
        1024 * 1024,
        schema,
        props,
        blocks_builder,
        path_location,
        Duration::from_secs(10),
        fs_builder,
    );

    HdfsOutputFormat::new(Box::new(writer_manager))
}

const SCHEMA_STR: &'static str = r#"
message WindowOutput {
    required binary Name (UTF8);
    required int64 SumValue;
}"#;

struct TestBlocks {
    schema_types: Vec<u8>,

    name: Vec<ByteArray>,
    sum: Vec<i64>,
}

impl TestBlocks {
    pub fn with_capacity(capacity: usize) -> Self {
        TestBlocks {
            schema_types: vec![],
            name: Vec::with_capacity(capacity),
            sum: Vec::with_capacity(capacity),
        }
    }

    pub fn set_record_schema_type(&mut self, field_types: Vec<u8>) {
        self.schema_types = field_types;
    }
}

impl From<(usize, Vec<u8>)> for TestBlocks {
    fn from((batch_size, schema): (usize, Vec<u8>)) -> Self {
        let mut blocks = TestBlocks::with_capacity(batch_size);
        blocks.set_record_schema_type(schema);
        blocks
    }
}

impl Blocks for TestBlocks {
    fn append(&mut self, mut record: Record) -> usize {
        let mut reader = record.as_reader(self.schema_types.as_slice());
        let name = reader.get_str(0).unwrap();
        let sum = reader.get_i64(1).unwrap();

        self.name.push(ByteArray::from(name.as_str()));
        self.sum.push(sum);

        self.name.len()
    }

    fn flush(&mut self) -> Vec<ColumnValues> {
        let mut row_values = Vec::new();
        row_values.push(ColumnValues::ByteArrayValues(self.name.as_slice()));
        row_values.push(ColumnValues::Int64Values(self.sum.as_slice()));
        row_values
    }
}

pub struct TestPathLocation {
    test_file: String,
}

impl TestPathLocation {
    pub fn new() -> Self {
        let dir = std::env::temp_dir();
        let test_file = dir.as_path().join("showcase");
        let test_file = test_file.as_path().to_str().unwrap().to_string();

        println!("location path: {}", test_file);
        TestPathLocation { test_file }
    }
}

impl PathLocation for TestPathLocation {
    fn path(&mut self, record: &mut Record, task_id: &TaskId) -> anyhow::Result<String> {
        let min_timestamp = record.trigger_window().unwrap().min_timestamp();
        let time_string = fmt_date_time(Duration::from_millis(min_timestamp), "%Y-%m-%dT%H_%M_00");
        let p = format!(
            "{}.{}_{}.{}.parquet",
            self.test_file.clone(),
            task_id.job_id().0,
            task_id.task_number(),
            time_string
        );

        Ok(p)
    }
}
