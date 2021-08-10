use std::fmt::{Debug, Formatter};

use crate::core::checkpoint::CheckpointFunction;
use crate::core::element::{Record, Schema};
use crate::core::function::{Context, InputFormat, InputSplit, InputSplitSource, NamedFunction};

pub fn vec_source(
    data: Vec<Record>,
    schema: &[u8],
) -> IteratorInputFormat<impl FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send>>
{
    let n = IteratorInputFormat::new(
        move |_input_split, context| {
            let num_tasks = context.task_id.num_tasks;
            let task_number = context.task_id.task_number;
            let task_data = if num_tasks == 1 {
                data
            } else {
                data.iter()
                    .enumerate()
                    .filter_map(|(index, record)| {
                        if index as u16 % num_tasks == task_number {
                            Some(record.clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            };
            Box::new(task_data.into_iter())
        },
        schema,
    );
    n
}

pub struct IteratorInputFormat<T>
where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send>,
{
    vec_builder: Option<T>,
    schema: Vec<u8>,

    input_split: Option<InputSplit>,
    context: Option<Context>,
}

impl<T> IteratorInputFormat<T>
where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send>,
{
    pub fn new(vec_builder: T, schema: &[u8]) -> Self {
        IteratorInputFormat {
            vec_builder: Some(vec_builder),
            schema: schema.to_vec(),
            input_split: None,
            context: None,
        }
    }
}

impl<T> InputSplitSource for IteratorInputFormat<T> where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send>
{
}

impl<T> InputFormat for IteratorInputFormat<T>
where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send>,
{
    fn open(&mut self, input_split: InputSplit, context: &Context) -> crate::core::Result<()> {
        self.input_split = Some(input_split);
        self.context = Some(context.clone());

        Ok(())
    }

    fn record_iter(&mut self) -> Box<dyn Iterator<Item = Record> + Send> {
        let vec_builder = self.vec_builder.take().unwrap();
        let input_split = self.input_split.take().unwrap();
        let context = self.context.take().unwrap();

        vec_builder(input_split, context)
    }

    fn close(&mut self) -> crate::core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: Schema) -> Schema {
        Schema::from(self.schema.as_slice())
    }
}

impl<T> NamedFunction for IteratorInputFormat<T>
where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send>,
{
    fn name(&self) -> &str {
        "IteratorInputFormat"
    }
}

impl<T> CheckpointFunction for IteratorInputFormat<T> where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send>
{
}

impl<T> Debug for IteratorInputFormat<T>
where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IteratorInputFormat")
    }
}
