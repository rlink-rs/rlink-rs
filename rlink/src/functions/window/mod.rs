use std::time::Duration;

use crate::core::checkpoint::{CheckpointFunction, CheckpointHandle, FunctionSnapshotContext};
use crate::core::function::NamedFunction;
use crate::core::window::{TWindow, TimeWindow, Window, WindowAssigner, WindowAssignerContext};

/// window offset
pub struct Offset {
    offset: i64,
}

impl Offset {
    pub fn forward(offset: Duration) -> Self {
        Offset {
            offset: offset.as_millis() as i64,
        }
    }

    pub fn back(offset: Duration) -> Self {
        Offset {
            offset: offset.as_millis() as i64 * -1,
        }
    }
}

#[derive(Debug)]
pub struct SlidingEventTimeWindows {
    size: u64,
    slide: u64,
    offset: i64,
}

impl SlidingEventTimeWindows {
    pub fn new(size: Duration, slide: Duration, offset: Option<Offset>) -> Self {
        let size = size.as_millis() as u64;
        let slide = slide.as_millis() as u64;
        let offset = offset.map(|x| x.offset).unwrap_or(0);

        if offset.abs() as u64 >= slide || size <= 0 {
            panic!(
                "SlidingEventTimeWindows parameters must satisfy offset.abs() < slide and size > 0"
            )
        }
        SlidingEventTimeWindows {
            size,
            slide,
            offset,
        }
    }
}

impl WindowAssigner for SlidingEventTimeWindows {
    fn assign_windows(&self, timestamp: u64, _context: WindowAssignerContext) -> Vec<Window> {
        let mut windows = Vec::with_capacity((self.size / self.slide) as usize);
        let mut last_start =
            TimeWindow::get_window_start_with_offset(timestamp, self.offset, self.slide);
        if last_start < 0 {
            last_start = 0;
        }

        let mut start = last_start;
        loop {
            if start > timestamp as i64 - self.size as i64 {
                if start >= 0 {
                    let window = TimeWindow::new(start as u64, start as u64 + self.size);
                    // info!("Create window: {}", window);
                    windows.push(Window::TimeWindow(window));
                }
                start -= self.slide as i64;
            } else {
                break;
            }
        }

        windows.sort_by_key(|x| x.min_timestamp());
        windows
    }
}

impl NamedFunction for SlidingEventTimeWindows {
    fn name(&self) -> &str {
        "SlidingEventTimeWindows"
    }
}

#[async_trait]
impl CheckpointFunction for SlidingEventTimeWindows {
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::core::window::{WindowAssigner, WindowAssignerContext};
    use crate::functions::window::{Offset, SlidingEventTimeWindows};
    use crate::utils::date_time::current_timestamp_millis;

    #[test]
    pub fn window_assigner_test() {
        let time_windows = SlidingEventTimeWindows::new(
            Duration::from_secs(24 * 3600),
            Duration::from_secs(24 * 3600),
            Some(Offset::back(Duration::from_secs(8 * 3600))),
        );

        let ts = current_timestamp_millis();
        println!("{}", ts);
        let windows = time_windows.assign_windows(ts, WindowAssignerContext {});

        println!("{:?}", windows);
    }
}
