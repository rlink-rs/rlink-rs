use std::marker::PhantomData;
use std::sync::Arc;

use parquet::column::writer::ColumnWriter;
use parquet::data_type::{ByteArray, FixedLenByteArray, Int96};
use parquet::file::properties::WriterPropertiesPtr;
use parquet::file::writer::{FileWriter, InMemoryWriteableCursor, SerializedFileWriter};
use parquet::schema::types::TypePtr;
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

pub struct ParquetBlockWriter {
    cursor: InMemoryWriteableCursor,

    writer: SerializedFileWriter<InMemoryWriteableCursor>,

    row_group_size: usize,
    max_bytes: i64,
    total_bytes: i64,
    converter: Arc<Box<dyn BlockConverter>>,
    blocks: Option<Box<dyn Blocks>>,
}

impl ParquetBlockWriter {
    pub fn new(
        row_group_size: usize,
        max_bytes: i64,
        schema: TypePtr,
        props: WriterPropertiesPtr,
        converter: Arc<Box<dyn BlockConverter>>,
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
            blocks: Some(blocks),
        }
    }

    fn flush_buffer(&mut self) -> anyhow::Result<i64> {
        if self.blocks.is_none() {
            return Ok(0);
        }

        let batch_values = self.blocks.as_mut().unwrap().flush();
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

impl BlockWriter for ParquetBlockWriter {
    fn open(&mut self) {}

    fn append(&mut self, record: Record) -> anyhow::Result<bool> {
        let block_size = self.blocks.as_mut().unwrap().append(record);
        if block_size < self.row_group_size {
            return Ok(false);
        }

        let byte_size = self.flush_buffer()?;

        self.total_bytes += byte_size;
        if self.total_bytes >= self.max_bytes {
            self.blocks = None;
            Ok(true)
        } else {
            self.blocks = Some(self.converter.create_batch(self.row_group_size));
            Ok(false)
        }
    }

    fn close(mut self) -> anyhow::Result<Vec<u8>> {
        self.flush_buffer()?;

        self.writer.close()?;

        // let cursor = self.cursor.clone();
        // cursor.into_inner()
        Ok(self.cursor.data())
    }
}
