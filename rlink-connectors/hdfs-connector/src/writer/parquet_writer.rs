use std::marker::PhantomData;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::column::writer::ColumnWriter;
use parquet::data_type::{ByteArray, FixedLenByteArray, Int96};
use parquet::file::properties::{WriterProperties, WriterPropertiesPtr, WriterVersion};
use parquet::file::writer::{FileWriter, InMemoryWriteableCursor, SerializedFileWriter};
use parquet::schema::types::Type;
use rlink::api::element::Record;

use crate::writer::BlockWriter;

pub enum ColumnValues<'a> {
    BoolValues(&'a [bool]),
    Int32Values(&'a [i32]),
    Int64Values(&'a [i64]),
    Int96Values(&'a [Int96]),
    FloatValues(&'a [f32]),
    DoubleValues(&'a [f64]),
    ByteArrayValues(&'a [ByteArray]),
    FixedLenByteArrayValues(&'a [FixedLenByteArray]),
}

pub trait Blocks: Send + Sync {
    fn append(&mut self, record: Record) -> usize;
    fn flush(&mut self) -> Vec<ColumnValues>;
}

pub trait BlockConverter: Send + Sync {
    fn create_batch(&self, batch_size: usize) -> Box<dyn Blocks>;
}

pub struct DefaultBlockConverter<T>
where
    T: From<usize> + Blocks + 'static,
{
    a: PhantomData<T>,
}

impl<T> DefaultBlockConverter<T>
where
    T: From<usize> + Blocks + 'static,
{
    pub fn new() -> Self {
        DefaultBlockConverter { a: PhantomData }
    }
}

impl<T> BlockConverter for DefaultBlockConverter<T>
where
    T: From<usize> + Blocks + 'static,
{
    fn create_batch(&self, batch_size: usize) -> Box<dyn Blocks> {
        let t: Box<dyn Blocks> = Box::new(T::from(batch_size));
        t
    }
}

pub struct ParquetWriter {
    cursor: InMemoryWriteableCursor,

    writer: SerializedFileWriter<InMemoryWriteableCursor>,

    row_group_size: usize,
    max_bytes: i64,
    total_bytes: i64,
    converter: Box<dyn BlockConverter>,
    blocks: Box<dyn Blocks>,
}

impl ParquetWriter {
    pub fn new(
        row_group_size: usize,
        max_bytes: i64,
        schema: Arc<Type>,
        props: WriterPropertiesPtr,
        converter: Box<dyn BlockConverter>,
    ) -> Self {
        let cursor = InMemoryWriteableCursor::default();
        let writer = SerializedFileWriter::new(cursor.clone(), schema, props).unwrap();

        let blocks = converter.create_batch(row_group_size);
        Self {
            writer,
            cursor,
            row_group_size,
            max_bytes,
            total_bytes: 0,
            converter,
            blocks,
        }
    }

    fn flush_buffer(&mut self, batch_values: Vec<ColumnValues>) -> anyhow::Result<i64> {
        let mut batch_values_iter = batch_values.into_iter();

        let mut row_group_writer = self.writer.next_row_group()?;
        while let Some(mut col_writer) = row_group_writer.next_column()? {
            let column_values = batch_values_iter
                .next()
                .ok_or(anyhow!("column inconsistency"))?;

            match col_writer {
                ColumnWriter::BoolColumnWriter(ref mut typed) => {
                    if let ColumnValues::BoolValues(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::Int32ColumnWriter(ref mut typed) => {
                    if let ColumnValues::Int32Values(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::Int64ColumnWriter(ref mut typed) => {
                    if let ColumnValues::Int64Values(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::Int96ColumnWriter(ref mut typed) => {
                    if let ColumnValues::Int96Values(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::FloatColumnWriter(ref mut typed) => {
                    if let ColumnValues::FloatValues(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::DoubleColumnWriter(ref mut typed) => {
                    if let ColumnValues::DoubleValues(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                    if let ColumnValues::ByteArrayValues(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::FixedLenByteArrayColumnWriter(ref mut typed) => {
                    if let ColumnValues::FixedLenByteArrayValues(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
            }

            row_group_writer.close_column(col_writer)?;
        }

        let metadata = row_group_writer.close()?;
        self.writer.close_row_group(row_group_writer)?;

        Ok(metadata.total_byte_size())
    }
}

impl BlockWriter for ParquetWriter {
    fn open(&mut self) {}

    fn append(&mut self, record: Record) -> anyhow::Result<bool> {
        let block_size = self.blocks.append(record);
        if block_size < self.row_group_size {
            return Ok(false);
        }

        let batch_values = self.blocks.flush();
        let mut batch_values_iter = batch_values.into_iter();

        let mut row_group_writer = self.writer.next_row_group()?;
        while let Some(mut col_writer) = row_group_writer.next_column()? {
            let column_values = batch_values_iter
                .next()
                .ok_or(anyhow!("column inconsistency"))?;

            match col_writer {
                ColumnWriter::BoolColumnWriter(ref mut typed) => {
                    if let ColumnValues::BoolValues(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::Int32ColumnWriter(ref mut typed) => {
                    if let ColumnValues::Int32Values(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::Int64ColumnWriter(ref mut typed) => {
                    if let ColumnValues::Int64Values(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::Int96ColumnWriter(ref mut typed) => {
                    if let ColumnValues::Int96Values(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::FloatColumnWriter(ref mut typed) => {
                    if let ColumnValues::FloatValues(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::DoubleColumnWriter(ref mut typed) => {
                    if let ColumnValues::DoubleValues(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::ByteArrayColumnWriter(ref mut typed) => {
                    if let ColumnValues::ByteArrayValues(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
                ColumnWriter::FixedLenByteArrayColumnWriter(ref mut typed) => {
                    if let ColumnValues::FixedLenByteArrayValues(values) = column_values {
                        typed.write_batch(values, None, None)?;
                    } else {
                        panic!("type inconsistency");
                    }
                }
            }

            row_group_writer.close_column(col_writer)?;
        }

        let metadata = row_group_writer.close()?;
        self.writer.close_row_group(row_group_writer)?;

        self.total_bytes += metadata.total_byte_size();
        if self.total_bytes >= self.max_bytes {
            self.writer.close()?;

            Ok(true)
        } else {
            self.blocks = self.converter.create_batch(self.row_group_size);
            Ok(false)
        }
    }

    fn close(self) -> Option<Vec<u8>> {
        let cursor = self.cursor.clone();
        // cursor.into_inner()
        Some(cursor.data())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;

    use parquet::basic::Compression;
    use parquet::data_type::ByteArray;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::schema::parser::parse_message_type;
    use rlink::api::element::Record;
    use rlink::utils::date_time::current_timestamp;

    use crate::writer::parquet_writer::{
        Blocks, ColumnValues, DefaultBlockConverter, ParquetWriter,
    };
    use crate::writer::BlockWriter;

    struct TestBlocks {
        capacity: usize,
        col0: Vec<i32>,
        col1: Vec<ByteArray>,
    }

    impl TestBlocks {
        pub fn with_capacity(capacity: usize) -> Self {
            TestBlocks {
                capacity,
                col0: Vec::with_capacity(capacity),
                col1: Vec::with_capacity(capacity),
            }
        }
    }

    impl From<usize> for TestBlocks {
        fn from(batch_size: usize) -> Self {
            TestBlocks::with_capacity(batch_size)
        }
    }

    impl Blocks for TestBlocks {
        fn append(&mut self, record: Record) -> usize {
            self.col0.push(1);
            self.col1.push(ByteArray::from("0123456789"));

            self.col0.len()
        }

        fn flush(&mut self) -> Vec<ColumnValues> {
            println!("flush");
            let mut row_values = Vec::new();
            row_values.push(ColumnValues::Int32Values(self.col0.as_slice()));
            row_values.push(ColumnValues::ByteArrayValues(self.col1.as_slice()));
            row_values
        }
    }

    #[test]
    pub fn writer_test() {
        let schema_str = r#"
message Document {
    required int32 DocId;
    required binary Context (UTF8);
}"#;
        let schema = Arc::new(parse_message_type(schema_str).unwrap());
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .build(),
        );
        let blocks = DefaultBlockConverter::<TestBlocks>::new();
        let blocks = Box::new(blocks);

        let mut writer = ParquetWriter::new(10000 * 10, 1024 * 10, schema, props, blocks);

        let begin = current_timestamp();
        let mut index = 0;
        while !writer.append(Record::new()).unwrap() {
            // println!("-");
            index += 1;
        }
        let end = current_timestamp();

        let bytes = writer.close();
        println!(
            "len: {}, loops: {}, ts: {}",
            bytes.as_ref().unwrap().len(),
            index,
            end.checked_sub(begin).unwrap().as_millis()
        );

        {
            let mut file = std::fs::File::create("test.parquet").expect("create failed");
            file.write_all(bytes.as_ref().unwrap().as_slice())
                .expect("write failed");
        }
    }
}
