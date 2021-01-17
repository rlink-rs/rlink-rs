use std::borrow::BorrowMut;
use std::fmt::Debug;

use crate::api::element::Record;
use crate::api::element::{types, BufferReader, BufferWriter};
use crate::api::function::{Context, Function, ReduceFunction};
use crate::functions::percentile::{get_percentile_capacity, Percentile};
use crate::functions::schema_base::FunctionSchema;

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
        value_reader: Option<&mut BufferReader>,
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
        value_reader: Option<&mut BufferReader>,
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
        value_reader: Option<&mut BufferReader>,
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
        value_reader: Option<&mut BufferReader>,
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
        value_reader: Option<&mut BufferReader>,
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
        value_reader: Option<&mut BufferReader>,
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
        value_reader: Option<&mut BufferReader>,
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
        value_reader: Option<&mut BufferReader>,
        value_index: usize,
        record_reader: &mut BufferReader,
    ) {
        let record_value = record_reader.get_i64(self.record_index).unwrap();
        match value_reader {
            Some(value_reader) => {
                let stat_value = value_reader.get_bytes_mut(value_index).unwrap();

                let mut percentile = Percentile::new(self.scale, stat_value);
                percentile.accumulate(record_value as f64);

                writer.set_bytes(stat_value).unwrap();
            }
            None => {
                let mut count_container = self.count_container.clone();
                let mut percentile = Percentile::new(self.scale, count_container.as_mut_slice());
                percentile.accumulate(record_value as f64);

                writer.set_bytes(count_container.as_slice()).unwrap();
            }
        }
    }
}

#[derive(Debug)]
pub struct SchemaBaseReduceFunction {
    field_types: Vec<u8>,
    val_field_types: Vec<u8>,

    val_len: usize,

    agg_operators: Vec<Box<dyn Aggregation>>,
}

impl SchemaBaseReduceFunction {
    pub fn new(agg_operators: Vec<Box<dyn Aggregation>>, field_types: &[u8]) -> Self {
        let val_field_types: Vec<u8> = agg_operators
            .iter()
            .map(|agg| {
                if agg.agg_type() != types::BYTES
                    && field_types[agg.record_index()] != agg.agg_type()
                {
                    panic!("column type check failure at {}", agg.record_index());
                }
                agg.agg_type()
            })
            .collect();
        let val_len: usize = agg_operators.iter().map(|x| x.len()).sum();

        // let val_len = val_data_types.len() * 8;
        SchemaBaseReduceFunction {
            field_types: field_types.to_vec(),
            val_field_types,
            val_len,
            agg_operators,
        }
    }
}

impl FunctionSchema for SchemaBaseReduceFunction {
    fn get_schema_types(&self) -> Vec<u8> {
        self.val_field_types.clone()
    }
}

impl ReduceFunction for SchemaBaseReduceFunction {
    fn open(&mut self, _context: &Context) -> crate::api::Result<()> {
        Ok(())
    }

    fn reduce(&self, value: Option<&mut Record>, record: &mut Record) -> Record {
        let mut record_rt = Record::with_capacity(self.val_len);
        let mut writer = record_rt.get_writer(self.val_field_types.as_slice());

        let mut record_reader = record.get_reader(self.field_types.as_slice());

        match value {
            Some(state_value) => {
                let mut stat_reader = state_value.get_reader(self.val_field_types.as_slice());

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

    fn close(&mut self) -> crate::api::Result<()> {
        Ok(())
    }
}

impl Function for SchemaBaseReduceFunction {
    fn get_name(&self) -> &str {
        "SchemaBaseReduceFunction"
    }
}
