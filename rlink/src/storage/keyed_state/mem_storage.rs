use dashmap::DashMap;

use crate::core::runtime::JobId;
use crate::core::window::Window;
use crate::storage::keyed_state::mem_reducing_state::MemoryReducingState;

lazy_static! {
    static ref DROP_WINDOW_STATE_STORAGE: DashMap<StorageKey, DashMap<Window, MemoryReducingState>> =
        DashMap::new();
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct StorageKey {
    job_id: JobId,
    task_number: u16,
}

impl StorageKey {
    pub fn new(job_id: JobId, task_number: u16) -> Self {
        StorageKey {
            job_id,
            task_number,
        }
    }
}

pub(crate) fn append_drop_window(
    storage_key: StorageKey,
    window: Window,
    state: MemoryReducingState,
) {
    let drop_window_states: &DashMap<StorageKey, DashMap<Window, MemoryReducingState>> =
        &*DROP_WINDOW_STATE_STORAGE;

    let task_storage = drop_window_states
        .entry(storage_key)
        .or_insert_with(|| DashMap::new());
    task_storage.value().insert(window, state);
}

pub(crate) fn remove_drop_window(
    job_id: JobId,
    task_number: u16,
    window: Window,
) -> Option<MemoryReducingState> {
    let drop_window_states: &DashMap<StorageKey, DashMap<Window, MemoryReducingState>> =
        &*DROP_WINDOW_STATE_STORAGE;

    let key = StorageKey::new(job_id, task_number);
    match drop_window_states.get(&key) {
        Some(task_storage) => task_storage.value().remove(&window).map(|(_k, v)| v),
        None => None,
    }
}
