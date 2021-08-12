use std::borrow::BorrowMut;
use std::fmt::Debug;

use crate::core::data_types::{DataType, Field, Schema};
use crate::core::element::{types, BufferMutReader, BufferReader, BufferWriter};
use crate::core::element::{FnSchema, Record};
use crate::core::function::{Context, NamedFunction, ReduceFunction};
use crate::functions::percentile::{get_percentile_capacity, PercentileWriter};
use std::convert::TryFrom;

pub fn sum_i64(column_index: usize) -> Box<dyn Aggregation> {
    let agg = SumI64 {
        record_index: column_index,
    };
    let agg: Box<dyn Aggregation> = Box::new(agg);
    agg
}

pub fn sum_f64(column_index: usize) -> Box<dyn Aggregation> {
    let agg = SumF64 {
        record_index: column_index,
    };
    let agg: Box<dyn Aggregation> = Box::new(agg);
    agg
}

pub fn max_i64(column_index: usize) -> Box<dyn Aggregation> {
    let agg = MaxI64 {
        record_index: column_index,
    };
    let agg: Box<dyn Aggregation> = Box::new(agg);
    agg
}

pub fn max_f64(column_index: usize) -> Box<dyn Aggregation> {
    let agg = MaxF64 {
        record_index: column_index,
    };
    let agg: Box<dyn Aggregation> = Box::new(agg);
    agg
}

pub fn min_i64(column_index: usize) -> Box<dyn Aggregation> {
    let agg = MinI64 {
        record_index: column_index,
    };
    let agg: Box<dyn Aggregation> = Box::new(agg);
    agg
}
pub fn min_f64(column_index: usize) -> Box<dyn Aggregation> {
    let agg = MinF64 {
        record_index: column_index,
    };
    let agg: Box<dyn Aggregation> = Box::new(agg);
    agg
}

pub fn pct_u64(column_index: usize, scale: &'static [f64]) -> Box<dyn Aggregation> {
    let mut count_container = Vec::with_capacity(get_percentile_capacity(scale));
    for _ in 0..count_container.capacity() {
        count_container.push(0);
    }

    let agg = PctU64 {
        record_index: column_index,
        scale,
        count_container,
    };
    let agg: Box<dyn Aggregation> = Box::new(agg);
    agg
}

pub trait Aggregation: Debug {
    fn agg_type(&self) -> u8;
    fn len(&self) -> usize;
    fn record_index(&self) -> usize;
    fn reduce(
        &self,
        writer: &mut BufferWriter,
        value_reader: Option<&mut BufferMutReader>,
        value_index: usize,
        record_reader: &mut BufferReader,
    );
}

#[derive(Debug)]
pub struct SumI64 {
    record_index: usize,
}

impl Aggregation for SumI64 {
    #[inline]
    fn agg_type(&self) -> u8 {
        types::I64
    }

    fn len(&self) -> usize {
        types::len(self.agg_type()) as usize
    }

    fn record_index(&self) -> usize {
        self.record_index
    }

    fn reduce(
        &self,
        writer: &mut BufferWriter,
        value_reader: Option<&mut BufferMutReader>,
        value_index: usize,
        record_reader: &mut BufferReader,
    ) {
        let record_value = record_reader.get_i64(self.record_index).unwrap();
        match value_reader {
            Some(value_reader) => {
                let agg_value = value_reader.get_i64(value_index).unwrap() + record_value;
                writer.set_i64(agg_value).unwrap();
            }
            None => {
                writer.set_i64(record_value).unwrap();
            }
        }
    }
}

#[derive(Debug)]
pub struct SumF64 {
    record_index: usize,
}
impl Aggregation for SumF64 {
    #[inline]
    fn agg_type(&self) -> u8 {
        types::F64
    }

    fn len(&self) -> usize {
        types::len(self.agg_type()) as usize
    }

    fn record_index(&self) -> usize {
        self.record_index
    }

    fn reduce(
        &self,
        writer: &mut BufferWriter,
        value_reader: Option<&mut BufferMutReader>,
        value_index: usize,
        record_reader: &mut BufferReader,
    ) {
        let record_value = record_reader.get_f64(self.record_index).unwrap();
        match value_reader {
            Some(value_reader) => {
                let agg_value = value_reader.get_f64(value_index).unwrap() + record_value;
                writer.set_f64(agg_value).unwrap();
            }
            None => {
                writer.set_f64(record_value).unwrap();
            }
        }
    }
}

#[derive(Debug)]
pub struct MaxI64 {
    record_index: usize,
}

impl Aggregation for MaxI64 {
    #[inline]
    fn agg_type(&self) -> u8 {
        types::I64
    }

    fn len(&self) -> usize {
        types::len(self.agg_type()) as usize
    }

    fn record_index(&self) -> usize {
        self.record_index
    }

    fn reduce(
        &self,
        writer: &mut BufferWriter,
        value_reader: Option<&mut BufferMutReader>,
        value_index: usize,
        record_reader: &mut BufferReader,
    ) {
        let record_value = record_reader.get_i64(self.record_index).unwrap();
        match value_reader {
            Some(value_reader) => {
                let stat_value = value_reader.get_i64(value_index).unwrap();
                let max_value = std::cmp::max(record_value, stat_value);
                writer.set_i64(max_value).unwrap();
            }
            None => {
                writer.set_i64(record_value).unwrap();
            }
        }
    }
}

#[derive(Debug)]
pub struct MaxF64 {
    record_index: usize,
}

impl Aggregation for MaxF64 {
    #[inline]
    fn agg_type(&self) -> u8 {
        types::F64
    }

    fn len(&self) -> usize {
        types::len(self.agg_type()) as usize
    }

    fn record_index(&self) -> usize {
        self.record_index
    }

    fn reduce(
        &self,
        writer: &mut BufferWriter,
        value_reader: Option<&mut BufferMutReader>,
        value_index: usize,
        record_reader: &mut BufferReader,
    ) {
        let record_value = record_reader.get_f64(self.record_index).unwrap();
        match value_reader {
            Some(value_reader) => {
                let stat_value = value_reader.get_f64(value_index).unwrap();
                let max_value = if record_value > stat_value {
                    record_value
                } else {
                    stat_value
                };
                writer.set_f64(max_value).unwrap();
            }
            None => {
                writer.set_f64(record_value).unwrap();
            }
        }
    }
}

#[derive(Debug)]
pub struct MinI64 {
    record_index: usize,
}

impl Aggregation for MinI64 {
    #[inline]
    fn agg_type(&self) -> u8 {
        types::I64
    }

    fn len(&self) -> usize {
        types::len(self.agg_type()) as usize
    }

    fn record_index(&self) -> usize {
        self.record_index
    }

    fn reduce(
        &self,
        writer: &mut BufferWriter,
        value_reader: Option<&mut BufferMutReader>,
        value_index: usize,
        record_reader: &mut BufferReader,
    ) {
        let record_value = record_reader.get_i64(self.record_index).unwrap();
        match value_reader {
            Some(value_reader) => {
                let stat_value = value_reader.get_i64(value_index).unwrap();
                let min_value = std::cmp::min(record_value, stat_value);
                writer.set_i64(min_value).unwrap();
            }
            None => {
                writer.set_i64(record_value).unwrap();
            }
        }
    }
}

#[derive(Debug)]
pub struct MinF64 {
    record_index: usize,
}

impl Aggregation for MinF64 {
    #[inline]
    fn agg_type(&self) -> u8 {
        types::F64
    }

    fn len(&self) -> usize {
        types::len(self.agg_type()) as usize
    }

    fn record_index(&self) -> usize {
        self.record_index
    }

    fn reduce(
        &self,
        writer: &mut BufferWriter,
        value_reader: Option<&mut BufferMutReader>,
        value_index: usize,
        record_reader: &mut BufferReader,
    ) {
        let record_value = record_reader.get_f64(self.record_index).unwrap();
        match value_reader {
            Some(value_reader) => {
                let stat_value = value_reader.get_f64(value_index).unwrap();
                let min_value = if record_value > stat_value {
                    stat_value
                } else {
                    record_value
                };
                writer.set_f64(min_value).unwrap();
            }
            None => {
                writer.set_f64(record_value).unwrap();
            }
        }
    }
}

#[derive(Debug)]
pub struct PctU64 {
    record_index: usize,
    scale: &'static [f64],
    count_container: Vec<u8>,
}

impl Aggregation for PctU64 {
    #[inline]
    fn agg_type(&self) -> u8 {
        types::BYTES
    }

    fn len(&self) -> usize {
        get_percentile_capacity(self.scale)
    }

    fn record_index(&self) -> usize {
        self.record_index
    }

    fn reduce(
        &self,
        writer: &mut BufferWriter,
        value_reader: Option<&mut BufferMutReader>,
        value_index: usize,
        record_reader: &mut BufferReader,
    ) {
        let record_value = record_reader.get_i64(self.record_index).unwrap();
        match value_reader {
            Some(value_reader) => {
                let stat_value = value_reader.get_bytes_mut(value_index).unwrap();

                let mut percentile = PercentileWriter::new(self.scale, stat_value);
                percentile.accumulate(record_value as f64);

                writer.set_bytes(stat_value).unwrap();
            }
            None => {
                let mut count_container = self.count_container.clone();
                let mut percentile =
                    PercentileWriter::new(self.scale, count_container.as_mut_slice());
                percentile.accumulate(record_value as f64);

                writer.set_bytes(count_container.as_slice()).unwrap();
            }
        }
    }
}

#[derive(Debug)]
pub struct SchemaReduceFunction {
    schema: Schema,
    val_schema: Schema,

    val_len: usize,

    agg_operators: Vec<Box<dyn Aggregation>>,
}

impl SchemaReduceFunction {
    pub fn new(agg_operators: Vec<Box<dyn Aggregation>>) -> Self {
        // let val_len = val_data_types.len() * 8;
        SchemaReduceFunction {
            schema: Schema::empty(),
            val_schema: Schema::empty(),
            val_len: 0,
            agg_operators,
        }
    }
}

impl ReduceFunction for SchemaReduceFunction {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        let schema = context.input_schema.first();
        let val_schema: Schema = self.schema(context.input_schema.clone()).into();
        let val_len: usize = self.agg_operators.iter().map(|x| x.len()).sum();

        self.schema = schema.clone();
        self.val_schema = val_schema;
        self.val_len = val_len;

        Ok(())
    }

    fn reduce(&self, value: Option<&mut Record>, record: &mut Record) -> Record {
        let mut record_rt = Record::with_capacity(self.val_len);
        let mut writer = record_rt.as_writer(self.val_schema.as_type_ids());

        let mut record_reader = record.as_reader(self.schema.as_type_ids());

        match value {
            Some(state_value) => {
                let mut stat_reader = state_value.as_reader_mut(self.val_schema.as_type_ids());

                for index in 0..self.agg_operators.len() {
                    self.agg_operators[index].reduce(
                        writer.borrow_mut(),
                        Some(stat_reader.borrow_mut()),
                        index,
                        record_reader.borrow_mut(),
                    )
                }
            }
            None => {
                for index in 0..self.agg_operators.len() {
                    self.agg_operators[index].reduce(
                        writer.borrow_mut(),
                        None,
                        index,
                        record_reader.borrow_mut(),
                    )
                }
            }
        }
        record_rt
    }

    fn close(&mut self) -> crate::core::Result<()> {
        Ok(())
    }

    fn schema(&self, input_schema: FnSchema) -> FnSchema {
        let schema = input_schema.first();
        let val_fields: Vec<Field> = self
            .agg_operators
            .iter()
            .map(|agg| {
                let field = schema.field(agg.record_index());
                let type_id = agg.agg_type();
                if type_id != types::BYTES && field.data_type_id() != type_id {
                    panic!("column type check failure at {}", agg.record_index());
                }
                Field::new("agg", DataType::try_from(type_id).unwrap())
            })
            .collect();

        FnSchema::Single(Schema::new(val_fields))
    }
}

impl NamedFunction for SchemaReduceFunction {
    fn name(&self) -> &str {
        "SchemaBaseReduceFunction"
    }
}
