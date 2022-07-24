use std::borrow::BorrowMut;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Add;

use crate::core::data_types::{DataType, Field, Schema};
use crate::core::element::{BufferMutReader, BufferReader, BufferWriter, FnSchema, Record};
use crate::core::function::{Context, NamedFunction, ReduceFunction};
use crate::functions::column_locate::{ColumnLocate, ColumnLocateBuilder};
use crate::functions::percentile::{get_percentile_capacity, PercentileWriter};

pub fn count() -> AggregationDescriptor {
    AggregationDescriptor::Count
}

pub fn sum<T: ColumnLocateBuilder>(column: T) -> AggregationDescriptor {
    AggregationDescriptor::Sum(column.build())
}

pub fn max<T: ColumnLocateBuilder>(column: T) -> AggregationDescriptor {
    AggregationDescriptor::Max(column.build())
}

pub fn min<T: ColumnLocateBuilder>(column: T) -> AggregationDescriptor {
    AggregationDescriptor::Min(column.build())
}

pub fn pct<T: ColumnLocateBuilder>(column: T, scale: &'static [f64]) -> AggregationDescriptor {
    AggregationDescriptor::Pct(column.build(), scale)
}

#[derive(Clone, Debug)]
pub enum AggregationDescriptor {
    Count,
    Sum(ColumnLocate),
    Max(ColumnLocate),
    Min(ColumnLocate),
    Pct(ColumnLocate, &'static [f64]),
}

impl AggregationDescriptor {
    pub fn to_aggregation(&self, schema: &Schema) -> Box<dyn Aggregation> {
        match self {
            Self::Count => {
                let agg = CountAggregation::new();
                Box::new(agg)
            }
            Self::Sum(column_locate) => {
                let (index, field) = column_locate.to_column(schema);
                create_basic_agg(index, BasicAggType::Sum, field.clone())
            }
            Self::Max(column_locate) => {
                let (index, field) = column_locate.to_column(schema);
                create_basic_agg(index, BasicAggType::Max, field.clone())
            }
            Self::Min(column_locate) => {
                let (index, field) = column_locate.to_column(schema);
                create_basic_agg(index, BasicAggType::Min, field.clone())
            }
            Self::Pct(column_locate, scale) => {
                let (index, field) = column_locate.to_column(schema);
                let agg = PctAggregation::new(index, field.clone(), scale);
                Box::new(agg)
            }
        }
    }
}

pub trait Aggregation: Debug {
    fn output_field(&self) -> &Field;
    fn len(&self) -> usize;
    fn reduce(
        &self,
        writer: &mut BufferWriter,
        value_reader: Option<&mut BufferMutReader>,
        value_index: usize,
        record_reader: &mut BufferReader,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CountAggregation {
    output_field: Field,
}

impl CountAggregation {
    pub fn new() -> Self {
        CountAggregation {
            output_field: Field::new("count", DataType::UInt64),
        }
    }
}

impl Aggregation for CountAggregation {
    fn output_field(&self) -> &Field {
        &self.output_field
    }

    fn len(&self) -> usize {
        self.output_field.len()
    }

    fn reduce(
        &self,
        writer: &mut BufferWriter,
        value_reader: Option<&mut BufferMutReader>,
        value_index: usize,
        _record_reader: &mut BufferReader,
    ) {
        let agg_value = match value_reader {
            Some(value_reader) => {
                let basic_value = value_reader.get_u64(value_index).unwrap();
                basic_value + 1
            }
            None => 1,
        };
        writer.set_u64(agg_value).unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn create_basic_agg(
    column_index: usize,
    agg_type: BasicAggType,
    input_field: Field,
) -> Box<dyn Aggregation> {
    match input_field.data_type() {
        DataType::Int8 => {
            let basic_agg = BasicAggregation::<i8>::new(column_index, agg_type, input_field);
            Box::new(basic_agg)
        }
        DataType::UInt8 => {
            let basic_agg = BasicAggregation::<u8>::new(column_index, agg_type, input_field);
            Box::new(basic_agg)
        }
        DataType::Int16 => {
            let basic_agg = BasicAggregation::<i16>::new(column_index, agg_type, input_field);
            Box::new(basic_agg)
        }
        DataType::UInt16 => {
            let basic_agg = BasicAggregation::<u16>::new(column_index, agg_type, input_field);
            Box::new(basic_agg)
        }
        DataType::Int32 => {
            let basic_agg = BasicAggregation::<i32>::new(column_index, agg_type, input_field);
            Box::new(basic_agg)
        }
        DataType::UInt32 => {
            let basic_agg = BasicAggregation::<u32>::new(column_index, agg_type, input_field);
            Box::new(basic_agg)
        }
        DataType::Int64 => {
            let basic_agg = BasicAggregation::<i64>::new(column_index, agg_type, input_field);
            Box::new(basic_agg)
        }
        DataType::UInt64 => {
            let basic_agg = BasicAggregation::<u64>::new(column_index, agg_type, input_field);
            Box::new(basic_agg)
        }
        DataType::Float32 => {
            let basic_agg = BasicAggregation::<f32>::new(column_index, agg_type, input_field);
            Box::new(basic_agg)
        }
        DataType::Float64 => {
            let basic_agg = BasicAggregation::<f64>::new(column_index, agg_type, input_field);
            Box::new(basic_agg)
        }
        _ => panic!("un-support DataType {:?}", input_field.data_type()),
    }
}

#[derive(Copy, Clone, Debug)]
pub enum BasicAggType {
    Sum,
    Max,
    Min,
}

impl Display for BasicAggType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sum => write!(f, "sum"),
            Self::Max => write!(f, "max"),
            Self::Min => write!(f, "min"),
        }
    }
}

#[derive(Debug)]
pub struct BasicAggregation<T: ValueAgg> {
    column_index: usize,
    agg_type: BasicAggType,

    #[allow(dead_code)]
    input_field: Field,
    output_field: Field,

    value_agg: T,
}

impl<T: ValueAgg> BasicAggregation<T> {
    pub fn new(column_index: usize, agg_type: BasicAggType, input_field: Field) -> Self {
        let output_field = Field::new(
            format!("{}({})", agg_type, input_field.name()).as_str(),
            input_field.data_type().clone(),
        );

        BasicAggregation {
            column_index,
            agg_type,
            input_field,
            output_field,
            value_agg: T::default(),
        }
    }
}

impl<T: ValueAgg> Aggregation for BasicAggregation<T> {
    #[inline]
    fn output_field(&self) -> &Field {
        &self.output_field
    }

    fn len(&self) -> usize {
        self.output_field.len()
    }

    fn reduce(
        &self,
        writer: &mut BufferWriter,
        value_reader: Option<&mut BufferMutReader>,
        value_index: usize,
        record_reader: &mut BufferReader,
    ) {
        let record_value = self.value_agg.read_record(record_reader, self.column_index);
        let agg_value = match value_reader {
            Some(value_reader) => {
                let basic_value = self.value_agg.read_value(value_reader, value_index);
                match self.agg_type {
                    BasicAggType::Sum => basic_value + record_value,
                    BasicAggType::Max => {
                        if basic_value > record_value {
                            basic_value
                        } else {
                            record_value
                        }
                    }
                    BasicAggType::Min => {
                        if basic_value > record_value {
                            record_value
                        } else {
                            basic_value
                        }
                    }
                }
            }
            None => record_value,
        };
        self.value_agg.write_record(writer, agg_value)
    }
}

pub trait ValueAgg: Add<Output = Self> + PartialOrd + Default + Debug {
    fn read_value(&self, value_reader: &mut BufferMutReader, index: usize) -> Self;
    fn read_record(&self, record_reader: &BufferReader, index: usize) -> Self;
    fn write_record(&self, writer: &mut BufferWriter, value: Self);
}

impl ValueAgg for i8 {
    fn read_value(&self, value_reader: &mut BufferMutReader, index: usize) -> Self {
        value_reader.get_i8(index).unwrap()
    }

    fn read_record(&self, record_reader: &BufferReader, index: usize) -> Self {
        record_reader.get_i8(index).unwrap()
    }

    fn write_record(&self, writer: &mut BufferWriter, value: Self) {
        writer.set_i8(value).unwrap()
    }
}

impl ValueAgg for u8 {
    fn read_value(&self, value_reader: &mut BufferMutReader, index: usize) -> Self {
        value_reader.get_u8(index).unwrap()
    }

    fn read_record(&self, record_reader: &BufferReader, index: usize) -> Self {
        record_reader.get_u8(index).unwrap()
    }

    fn write_record(&self, writer: &mut BufferWriter, value: Self) {
        writer.set_u8(value).unwrap()
    }
}

impl ValueAgg for i16 {
    fn read_value(&self, value_reader: &mut BufferMutReader, index: usize) -> Self {
        value_reader.get_i16(index).unwrap()
    }

    fn read_record(&self, record_reader: &BufferReader, index: usize) -> Self {
        record_reader.get_i16(index).unwrap()
    }

    fn write_record(&self, writer: &mut BufferWriter, value: Self) {
        writer.set_i16(value).unwrap()
    }
}

impl ValueAgg for u16 {
    fn read_value(&self, value_reader: &mut BufferMutReader, index: usize) -> Self {
        value_reader.get_u16(index).unwrap()
    }

    fn read_record(&self, record_reader: &BufferReader, index: usize) -> Self {
        record_reader.get_u16(index).unwrap()
    }

    fn write_record(&self, writer: &mut BufferWriter, value: Self) {
        writer.set_u16(value).unwrap()
    }
}

impl ValueAgg for i32 {
    fn read_value(&self, value_reader: &mut BufferMutReader, index: usize) -> Self {
        value_reader.get_i32(index).unwrap()
    }

    fn read_record(&self, record_reader: &BufferReader, index: usize) -> Self {
        record_reader.get_i32(index).unwrap()
    }

    fn write_record(&self, writer: &mut BufferWriter, value: Self) {
        writer.set_i32(value).unwrap()
    }
}

impl ValueAgg for u32 {
    fn read_value(&self, value_reader: &mut BufferMutReader, index: usize) -> Self {
        value_reader.get_u32(index).unwrap()
    }

    fn read_record(&self, record_reader: &BufferReader, index: usize) -> Self {
        record_reader.get_u32(index).unwrap()
    }

    fn write_record(&self, writer: &mut BufferWriter, value: Self) {
        writer.set_u32(value).unwrap()
    }
}

impl ValueAgg for i64 {
    fn read_value(&self, value_reader: &mut BufferMutReader, index: usize) -> Self {
        value_reader.get_i64(index).unwrap()
    }

    fn read_record(&self, record_reader: &BufferReader, index: usize) -> Self {
        record_reader.get_i64(index).unwrap()
    }

    fn write_record(&self, writer: &mut BufferWriter, value: Self) {
        writer.set_i64(value).unwrap()
    }
}

impl ValueAgg for u64 {
    fn read_value(&self, value_reader: &mut BufferMutReader, index: usize) -> Self {
        value_reader.get_u64(index).unwrap()
    }

    fn read_record(&self, record_reader: &BufferReader, index: usize) -> Self {
        record_reader.get_u64(index).unwrap()
    }

    fn write_record(&self, writer: &mut BufferWriter, value: Self) {
        writer.set_u64(value).unwrap()
    }
}

impl ValueAgg for f32 {
    fn read_value(&self, value_reader: &mut BufferMutReader, index: usize) -> Self {
        value_reader.get_f32(index).unwrap()
    }

    fn read_record(&self, record_reader: &BufferReader, index: usize) -> Self {
        record_reader.get_f32(index).unwrap()
    }

    fn write_record(&self, writer: &mut BufferWriter, value: Self) {
        writer.set_f32(value).unwrap()
    }
}

impl ValueAgg for f64 {
    fn read_value(&self, value_reader: &mut BufferMutReader, index: usize) -> Self {
        value_reader.get_f64(index).unwrap()
    }

    fn read_record(&self, record_reader: &BufferReader, index: usize) -> Self {
        record_reader.get_f64(index).unwrap()
    }

    fn write_record(&self, writer: &mut BufferWriter, value: Self) {
        writer.set_f64(value).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PctAggregation {
    column_index: usize,
    scale: &'static [f64],
    count_container: Vec<u8>,

    #[allow(dead_code)]
    input_field: Field,
    output_field: Field,
}

impl PctAggregation {
    pub fn new(column_index: usize, input_field: Field, scale: &'static [f64]) -> Self {
        let output_field = Field::new(
            format!("pct({})", input_field.name()).as_str(),
            DataType::Binary,
        );

        let mut count_container = Vec::with_capacity(get_percentile_capacity(scale));
        for _ in 0..count_container.capacity() {
            count_container.push(0);
        }

        PctAggregation {
            column_index,
            scale,
            count_container,
            input_field,
            output_field,
        }
    }
}

impl Aggregation for PctAggregation {
    fn len(&self) -> usize {
        get_percentile_capacity(self.scale)
    }

    fn output_field(&self) -> &Field {
        &self.output_field
    }

    fn reduce(
        &self,
        writer: &mut BufferWriter,
        value_reader: Option<&mut BufferMutReader>,
        value_index: usize,
        record_reader: &mut BufferReader,
    ) {
        let record_value = record_reader.get_i64(self.column_index).unwrap();
        match value_reader {
            Some(value_reader) => {
                let stat_value = value_reader.get_binary_mut(value_index).unwrap();

                let mut percentile = PercentileWriter::new(self.scale, stat_value);
                percentile.accumulate(record_value as f64);

                writer.set_binary(stat_value).unwrap();
            }
            None => {
                let mut count_container = self.count_container.clone();
                let mut percentile =
                    PercentileWriter::new(self.scale, count_container.as_mut_slice());
                percentile.accumulate(record_value as f64);

                writer.set_binary(count_container.as_slice()).unwrap();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct SchemaReduceFunction {
    parallelism: u16,

    schema: Schema,
    val_schema: Schema,

    val_len: usize,

    agg_descriptors: Vec<AggregationDescriptor>,
    agg_operators: Vec<Box<dyn Aggregation>>,
}

impl SchemaReduceFunction {
    pub fn new(agg_descriptors: Vec<AggregationDescriptor>, parallelism: u16) -> Self {
        // let val_len = val_data_types.len() * 8;
        SchemaReduceFunction {
            parallelism,
            schema: Schema::empty(),
            val_schema: Schema::empty(),
            val_len: 0,
            agg_descriptors,
            agg_operators: vec![],
        }
    }

    fn agg_schema(&self, schema: &Schema) -> (Vec<Box<dyn Aggregation>>, Vec<Field>, usize) {
        let mut aggs = Vec::new();
        let mut fields = Vec::new();
        let mut len = 0;

        for agg_descriptor in &self.agg_descriptors {
            let agg = agg_descriptor.to_aggregation(schema);

            len += agg.len();
            fields.push(agg.output_field().clone());
            aggs.push(agg);
        }
        (aggs, fields, len)
    }
}

impl ReduceFunction for SchemaReduceFunction {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        self.schema = context.input_schema.first().clone();
        self.val_schema = self.schema(context.input_schema.clone()).into();

        let (agg_operators, _fields, val_len) = self.agg_schema(&self.schema);
        self.val_len = val_len;
        self.agg_operators = agg_operators;

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
        let val_fields: Vec<Field> = self.agg_schema(schema).1;

        FnSchema::Single(Schema::new(val_fields))
    }

    fn parallelism(&self) -> u16 {
        self.parallelism
    }
}

impl NamedFunction for SchemaReduceFunction {
    fn name(&self) -> &str {
        "SchemaBaseReduceFunction"
    }
}
