use crate::core::backend::KeyedStateBackend;
use crate::core::checkpoint::CheckpointFunction;
use crate::core::element::{Element, FnSchema, Record};
use crate::core::function::{Context, FlatMapFunction, NamedFunction};
use crate::core::properties::SystemProperties;
use crate::core::runtime::JobId;
use crate::storage::keyed_state::{ReducingState, StateKey, TReducingState};

pub(crate) struct KeyedStateFlatMapFunction {
    parent_job_id: JobId,
    task_number: u16,

    state_mode: KeyedStateBackend,
}

impl KeyedStateFlatMapFunction {
    pub fn new() -> Self {
        KeyedStateFlatMapFunction {
            parent_job_id: JobId::default(),
            task_number: 0,
            state_mode: KeyedStateBackend::Memory,
        }
    }
}

impl FlatMapFunction for KeyedStateFlatMapFunction {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
        if context.parents.len() != 1 {
            panic!("KeyedStateMap job can only one parent");
        }

        self.parent_job_id = context.parents[0].0.task_id.job_id;
        self.task_number = context.task_id.task_number;

        if let Ok(state_mode) = context.application_properties.get_keyed_state_backend() {
            self.state_mode = state_mode;
        }

        Ok(())
    }

    fn flat_map(&mut self, _record: Record) -> Box<dyn Iterator<Item = Record>> {
        unimplemented!()
    }

    fn flat_map_element(&mut self, element: Element) -> Box<dyn Iterator<Item = Element>> {
        let record = element.into_record();
        if record.len() > 0 {
            panic!("drop window's Record is no value");
        }
        if record.trigger_window.is_none() {
            panic!("drop window not found");
        }

        let window = record.trigger_window.unwrap();

        let state_key = StateKey::new(window.clone(), self.parent_job_id, self.task_number);
        let reducing_state = ReducingState::new(&state_key, self.state_mode);
        match reducing_state {
            Some(reducing_state) => {
                let state_iter = reducing_state.iter();
                Box::new(state_iter.map(|record| Element::Record(record)))
                // Box::new(BatchIterator::new(state_iter, window))
            }
            None => Box::new(vec![].into_iter()),
        }
    }

    fn close(&mut self) -> crate::core::Result<()> {
        Ok(())
    }

    fn schema(&self, input_schema: FnSchema) -> FnSchema {
        let (_flag_record, reduce_schema): (Vec<u8>, Vec<u8>) = input_schema.into();
        FnSchema::Single(reduce_schema)
    }
}

impl NamedFunction for KeyedStateFlatMapFunction {
    fn name(&self) -> &str {
        "KeyedStateFlatMapFunction"
    }
}

impl CheckpointFunction for KeyedStateFlatMapFunction {}

// pub(crate) struct BatchIterator<T>
// where
//     T: Iterator<Item = Record>,
// {
//     end: bool,
//     iterator: T,
//     window: Window,
// }
//
// impl<T> BatchIterator<T>
// where
//     T: Iterator<Item = Record>,
// {
//     pub fn new(iterator: T, window: Window) -> Self {
//         BatchIterator {
//             end: false,
//             iterator,
//             window,
//         }
//     }
// }
//
// impl<T> Iterator for BatchIterator<T>
// where
//     T: Iterator<Item = Record>,
// {
//     type Item = Element;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         if self.end {
//             return None;
//         }
//
//         match self.iterator.next() {
//             Some(n) => Some(Element::Record(n)),
//             None => {
//                 self.end = true;
//
//                 let window_finish_barrier = Barrier::new(CheckpointId(self.window.min_timestamp()));
//                 Some(Element::Barrier(window_finish_barrier))
//             }
//         }
//     }
// }
