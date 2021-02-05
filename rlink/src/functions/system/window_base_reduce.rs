use crate::api::backend::KeyedStateBackend;
use crate::api::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use crate::api::element::Record;
use crate::api::function::{BaseReduceFunction, Context, Function, ReduceFunction};
use crate::api::properties::SystemProperties;
use crate::api::window::TWindow;
use crate::storage::keyed_state::{TWindowState, WindowState};
use crate::utils::date_time::timestamp_str;

pub(crate) struct WindowBaseReduceFunction {
    reduce: Box<dyn ReduceFunction>,

    state: Option<WindowState>,
}

impl WindowBaseReduceFunction {
    pub fn new(reduce: Box<dyn ReduceFunction>) -> Self {
        WindowBaseReduceFunction {
            reduce,
            state: None,
        }
    }
}

impl BaseReduceFunction for WindowBaseReduceFunction {
    fn open(&mut self, context: &Context) -> crate::api::Result<()> {
        let task_id = context.task_id;
        let application_id = context.application_id.clone();

        let state_mode = context
            .application_properties
            .get_keyed_state_backend()
            .unwrap_or(KeyedStateBackend::Memory);
        self.state = Some(WindowState::new(
            application_id,
            task_id.job_id(),
            task_id.task_number(),
            state_mode,
        ));
        Ok(())
    }

    fn reduce(&mut self, key: Record, record: Record) {
        let state = self.state.as_mut().unwrap();
        let reduce_func = &self.reduce;
        state.merge(key, record, |val1, val2| reduce_func.reduce(val1, val2));
    }

    fn drop_state(&mut self, watermark_timestamp: u64) -> Vec<Record> {
        let state = self.state.as_mut().unwrap();
        let mut drop_windows = Vec::new();
        for window in state.windows() {
            if window.max_timestamp() <= watermark_timestamp {
                drop_windows.push(window.clone());
                state.drop_window(&window);
            }
        }

        if drop_windows.len() > 0 {
            debug!(
                "check window for drop, trigger watermark={}, drop window size={}",
                timestamp_str(watermark_timestamp),
                drop_windows.len()
            );

            drop_windows.sort_by_key(|w| w.max_timestamp());

            drop_windows
                .into_iter()
                .map(|drop_window| {
                    let mut drop_record = Record::new();
                    drop_record.trigger_window = Some(drop_window);
                    drop_record
                })
                .collect()
        } else {
            vec![]
        }
    }

    fn close(&mut self) -> crate::api::Result<()> {
        Ok(())
    }

    fn checkpoint_function(&mut self) -> Option<Box<&mut dyn CheckpointFunction>> {
        Some(Box::new(self))
    }
}

impl Function for WindowBaseReduceFunction {
    fn name(&self) -> &str {
        "WindowBaseReduceFunction"
    }
}

impl CheckpointFunction for WindowBaseReduceFunction {
    fn initialize_state(
        &mut self,
        _context: &FunctionSnapshotContext,
        _handle: &Option<CheckpointHandle>,
    ) {
    }

    fn snapshot_state(&mut self, _context: &FunctionSnapshotContext) -> CheckpointHandle {
        let windows = self.state.as_ref().unwrap().windows();
        CheckpointHandle {
            handle: serde_json::to_string(&windows).unwrap(),
        }
    }
}
