use std::fmt::{Debug, Formatter};

use crate::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use crate::core::data_types::Schema;
use crate::core::element::{FnSchema, Record};
use crate::core::function::{
    Context, InputFormat, InputSplit, InputSplitSource, NamedFunction, SendableElementStream,
};
use crate::utils::stream::IteratorStream;

pub fn vec_source(
    data: Vec<Record>,
    schema: Schema,
    parallelism: u16,
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
        parallelism,
    );
    n
}

pub struct IteratorInputFormat<T>
where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send> + Send + Sync,
{
    parallelism: u16,

    vec_builder: Option<T>,
    schema: Schema,

    input_split: Option<InputSplit>,
    context: Option<Context>,
}

impl<T> IteratorInputFormat<T>
where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send> + Send + Sync,
{
    pub fn new(vec_builder: T, schema: Schema, parallelism: u16) -> Self {
        IteratorInputFormat {
            parallelism,
            vec_builder: Some(vec_builder),
            schema,
            input_split: None,
            context: None,
        }
    }
}

impl<T> InputSplitSource for IteratorInputFormat<T> where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send> + Send + Sync
{
}

#[async_trait]
impl<T> InputFormat for IteratorInputFormat<T>
where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send> + Send + Sync,
{
    async fn open(
        &mut self,
        input_split: InputSplit,
        context: &Context,
    ) -> crate::core::Result<()> {
        self.input_split = Some(input_split);
        self.context = Some(context.clone());

        Ok(())
    }

    async fn element_stream(&mut self) -> SendableElementStream {
        let vec_builder = self.vec_builder.take().unwrap();
        let input_split = self.input_split.take().unwrap();
        let context = self.context.take().unwrap();

        let itr = vec_builder(input_split, context);
        Box::pin(IteratorStream::new(itr))
    }

    async fn close(&mut self) -> crate::core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::from(&self.schema)
    }

    fn parallelism(&self) -> u16 {
        self.parallelism
    }
}

impl<T> NamedFunction for IteratorInputFormat<T>
where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send> + Send + Sync,
{
    fn name(&self) -> &str {
        "IteratorInputFormat"
    }
}

#[async_trait]
impl<T> CheckpointFunction for IteratorInputFormat<T>
where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send> + Send + Sync,
{
    async fn initialize_state(
        &mut self,
        _context: &FunctionSnapshotContext,
        _handle: &Option<CheckpointHandle>,
    ) {
    }

    async fn snapshot_state(
        &mut self,
        _context: &FunctionSnapshotContext,
    ) -> Option<CheckpointHandle> {
        None
    }
}

impl<T> Debug for IteratorInputFormat<T>
where
    T: FnOnce(InputSplit, Context) -> Box<dyn Iterator<Item = Record> + Send> + Send + Sync,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "IteratorInputFormat")
    }
}
