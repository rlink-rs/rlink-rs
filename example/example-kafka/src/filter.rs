use rlink::core::{function::FilterFunction, window::TWindow};

#[derive(Debug, Function)]
pub struct RangeFilterFunction {
    left: u64,
    right: u64,
}

impl RangeFilterFunction {
    #[allow(dead_code)]
    pub fn new(left: u64, right: u64) -> Self {
        if left <= right {
            panic!("specified start({}) must gh end({})", left, right)
        }
        Self { left, right }
    }
}

impl FilterFunction for RangeFilterFunction {
    fn open(&mut self, _context: &rlink::core::function::Context) -> rlink::core::Result<()> {
        Ok(())
    }

    fn filter(&self, record: &mut rlink::core::element::Record) -> bool {
        if let Some(window) = record.trigger_window() {
            return window.min_timestamp() >= self.left || window.max_timestamp() <= self.right;
        }
        return false;
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        Ok(())
    }
}
