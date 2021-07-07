use std::borrow::BorrowMut;
use std::collections::{BTreeMap, HashMap};

use crate::core::backend::KeyedStateBackend;
use crate::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use crate::core::element::Record;
use crate::core::function::{BaseReduceFunction, Context, NamedFunction, ReduceFunction};
use crate::core::properties::SystemProperties;
use crate::core::runtime::CheckpointId;
use crate::core::window::{TWindow, Window};
use crate::runtime::worker::runnable::reduce_runnable::ReduceCheckpointHandle;
use crate::storage::keyed_state::{TWindowState, WindowState};
use crate::utils::date_time::timestamp_str;

pub(crate) struct WindowBaseReduceFunction {
    reduce: Box<dyn ReduceFunction>,

    state: Option<WindowState>,

    window_checkpoints: BTreeMap<CheckpointId, HashMap<Window, bool>>,
    skip_windows: Vec<Window>,
}

impl WindowBaseReduceFunction {
    pub fn new(reduce: Box<dyn ReduceFunction>) -> Self {
        WindowBaseReduceFunction {
            reduce,
            state: None,
            window_checkpoints: BTreeMap::new(),
            skip_windows: Vec::new(),
        }
    }

    fn filter_skip_window(&self, windows: &mut Vec<Window>) -> Vec<Window> {
        windows
            .iter()
            .filter(|w| !self.can_skip_window(w))
            .map(|w| w.clone())
            .collect()
    }

    fn can_skip_window(&self, window: &Window) -> bool {
        self.skip_windows
            .iter()
            .find(|x| {
                x.max_timestamp() == window.max_timestamp()
                    && x.min_timestamp() == window.min_timestamp()
            })
            .is_some()
    }
}

impl BaseReduceFunction for WindowBaseReduceFunction {
    fn open(&mut self, context: &Context) -> crate::core::Result<()> {
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
        self.initialize_state(&context.checkpoint_context(), &context.checkpoint_handle);
        Ok(())
    }

    fn reduce(&mut self, key: Record, mut record: Record) {
        // check skip window
        if self.skip_windows.len() > 0 {
            if let Some(windows) = record.location_windows.borrow_mut() {
                let filter_windows = self.filter_skip_window(windows);
                if filter_windows.len() == 0 {
                    return;
                }

                record.location_windows = Some(filter_windows);
            }
        }

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

            self.window_checkpoints
                .iter_mut()
                .for_each(|(_checkpoint_id, windows)| {
                    drop_windows.iter().for_each(|w| {
                        windows.get_mut(w).map(|x| *x = true);
                    });
                });

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

    fn close(&mut self) -> crate::core::Result<()> {
        Ok(())
    }
}

impl NamedFunction for WindowBaseReduceFunction {
    fn name(&self) -> &str {
        "WindowBaseReduceFunction"
    }
}

impl CheckpointFunction for WindowBaseReduceFunction {
    fn initialize_state(
        &mut self,
        _context: &FunctionSnapshotContext,
        handle: &Option<CheckpointHandle>,
    ) {
        if let Some(handle) = handle {
            let handle = ReduceCheckpointHandle::from(handle.handle.as_str());
            let current_windows = handle.into_windows();

            self.skip_windows = current_windows;
            info!("skip windows: {:?}", self.skip_windows)
        }
    }

    fn snapshot_state(&mut self, context: &FunctionSnapshotContext) -> Option<CheckpointHandle> {
        let windows = self.state.as_ref().unwrap().windows();
        let mut windows_map = HashMap::with_capacity(windows.len());
        windows.iter().for_each(|w| {
            windows_map.insert(w.clone(), false);
        });
        self.window_checkpoints
            .insert(context.checkpoint_id, windows_map);

        let completed_checkpoint_ids: Vec<CheckpointId> = self
            .window_checkpoints
            .iter()
            .filter_map(|(checkpoint_id, windows)| {
                let c = windows
                    .iter()
                    .filter(|(_w, is_completed)| !(**is_completed))
                    .count();
                if c == 0 {
                    Some(*checkpoint_id)
                } else {
                    None
                }
            })
            .collect();

        for checkpoint_id in &completed_checkpoint_ids {
            self.window_checkpoints.remove(checkpoint_id);
        }

        // memory protected against oom! delete oldest checkpoints in `window_checkpoints`
        if self.window_checkpoints.len() > 100 {
            let min_checkpoint_id = self
                .window_checkpoints
                .iter()
                .map(|(ck_id, _m)| *ck_id)
                .min_by_key(|ck_id| *ck_id)
                .unwrap();
            self.window_checkpoints.remove(&min_checkpoint_id);
        }

        let max_checkpoint_id = completed_checkpoint_ids
            .iter()
            .max_by_key(|x| x.0)
            .map(|x| *x);
        let handle = ReduceCheckpointHandle::new(max_checkpoint_id, windows).to_string();

        Some(CheckpointHandle { handle })
    }
}
