use rlink::core;
use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{Context, FlatMapFunction};
use rlink::functions::percentile::PercentileReader;
use rlink_example_utils::buffer_gen::output;

#[derive(Debug, Function)]
pub struct OutputMapFunction {
    data_type: Vec<u8>,
    scala: &'static [f64],
}

impl OutputMapFunction {
    pub fn new(scala: &'static [f64]) -> Self {
        OutputMapFunction {
            data_type: vec![],
            scala,
        }
    }
}

impl FlatMapFunction for OutputMapFunction {
    fn open(&mut self, context: &Context) -> core::Result<()> {
        self.data_type = context.input_schema.first().to_vec();
        Ok(())
    }

    fn flat_map(&mut self, mut record: Record) -> Box<dyn Iterator<Item = Record>> {
        let reader = record.as_reader(self.data_type.as_slice());

        let percentile_buffer = reader.get_bytes(2).unwrap();
        let percentile = PercentileReader::new(self.scala, percentile_buffer);
        let pct_99 = percentile.get_result(99) as i64;
        let pct_90 = percentile.get_result(90) as i64;

        let output = output::Entity {
            field: reader.get_str(0).unwrap(),
            value: reader.get_i64(1).unwrap(),
            pct_99,
            pct_90,
        };
        let mut output_record = Record::new();
        output.to_buffer(output_record.as_buffer()).unwrap();
        output_record.set_window_trigger(record.trigger_window().unwrap());

        Box::new(vec![output_record].into_iter())
    }

    fn close(&mut self) -> core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::from(&output::FIELD_TYPE[..])
    }
}
