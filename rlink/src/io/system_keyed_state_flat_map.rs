use crate::api::backend::KeyedStateBackend;
use crate::api::element::Record;
use crate::api::function::{Context, Function, MapFunction};
use crate::api::properties::SystemProperties;
use crate::storage::keyed_state::{ReducingState, ReducingStateWrap, StateKey};

pub struct SystemKeyedStateMapFunction {
    parent_job_id: u32,
    task_number: u16,

    state_mode: KeyedStateBackend,
}

impl SystemKeyedStateMapFunction {
    pub fn new() -> Self {
        SystemKeyedStateMapFunction {
            parent_job_id: 0,
            task_number: 0,
            state_mode: KeyedStateBackend::Memory,
        }
    }
}

impl MapFunction for SystemKeyedStateMapFunction {
    fn open(&mut self, context: &Context) {
        if context.parents.len() != 1 {
            panic!("KeyedStateMap job can only one parent");
        }

        self.parent_job_id = context.parents[0].0.task_id.job_id;
        self.task_number = context.task_id.task_number;

        if let Ok(state_mode) = context.application_properties.get_keyed_state_backend() {
            self.state_mode = state_mode;
        }
    }

    fn map(&mut self, record: Record) -> Box<dyn Iterator<Item = Record>> {
        if record.len() > 0 {
            panic!("drop window's Record is no value");
        }
        if record.trigger_window.is_none() {
            panic!("drop window not found");
        }

        let window = record.trigger_window.unwrap();

        let state_key = StateKey::new(window.clone(), self.parent_job_id, self.task_number);
        let reducing_state = ReducingStateWrap::new(&state_key, self.state_mode);
        match reducing_state {
            Some(reducing_state) => {
                let state_iter = reducing_state.iter();
                Box::new(state_iter)
            }
            None => Box::new(vec![].into_iter()),
        }
    }

    fn close(&mut self) {}
}

impl Function for SystemKeyedStateMapFunction {
    fn get_name(&self) -> &str {
        "SystemKeyedStateMapFunction"
    }
}
