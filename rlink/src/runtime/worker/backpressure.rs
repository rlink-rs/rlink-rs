use std::cell::RefCell;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use crate::utils::date_time::current_timestamp_millis;

#[derive(Clone, Debug)]
pub struct Backpressure {
    raw: Rc<RefCell<RawBackpressure>>,
    pause_dur: Arc<AtomicU64>,
}

impl Backpressure {
    pub fn new() -> Self {
        Backpressure {
            raw: Rc::new(RefCell::new(RawBackpressure::new())),
            pause_dur: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn register(&self, event_time_backpressure: EventTimeBackpressure) {
        self.raw.borrow_mut().register(event_time_backpressure);
    }

    pub fn filter(&self, event_timestamp: u64) {
        self.raw.borrow_mut().filter(event_timestamp);
    }

    pub fn take(&self) -> Option<Duration> {
        self.raw.borrow_mut().take()
    }
}

#[derive(Clone, Debug)]
pub struct RawBackpressure {
    event_time_backpressure: Option<EventTimeBackpressure>,
    pause_time: Option<Duration>,
}

impl RawBackpressure {
    pub fn new() -> Self {
        RawBackpressure {
            event_time_backpressure: None,
            pause_time: None,
        }
    }

    pub fn register(&mut self, event_time_backpressure: EventTimeBackpressure) {
        self.event_time_backpressure = Some(event_time_backpressure);
    }

    pub fn filter(&mut self, event_timestamp: u64) {
        self.pause_time = match self.event_time_backpressure.as_mut() {
            Some(bp) => bp.filter(event_timestamp),
            None => None,
        };
    }

    pub fn take(&mut self) -> Option<Duration> {
        if self.pause_time.is_some() {
            self.pause_time.take()
        } else {
            None
        }
    }
}

/// The maximum range of `event_timestamp` that can be processed in a `process_timestamp` range
#[derive(Clone, Debug)]
pub struct EventTimeBackpressure {
    process_timestamp: i64,
    event_timestamp: i64,

    period: i64,
    max_times_period: i64,
    pause_time: Duration,
}

impl EventTimeBackpressure {
    pub fn new(period: Duration, max_times: usize) -> Self {
        let pause_time = period / 2;
        let period = period.as_millis() as i64;
        EventTimeBackpressure {
            process_timestamp: 0,
            event_timestamp: 0,

            period,
            max_times_period: max_times as i64 * period,
            pause_time,
        }
    }

    pub fn filter(&mut self, event_timestamp: u64) -> Option<Duration> {
        if self.limit_check(event_timestamp as i64) {
            Some(self.pause_time)
        } else {
            None
        }
    }

    fn limit_check(&mut self, event_timestamp: i64) -> bool {
        if self.process_timestamp == 0 {
            self.process_timestamp = current_timestamp_millis() as i64;
            self.event_timestamp = event_timestamp;
        } else {
            if event_timestamp - self.event_timestamp > self.max_times_period {
                let old_process_timestamp = self.process_timestamp;
                let current_timestamp = current_timestamp_millis() as i64;

                self.process_timestamp = current_timestamp;
                self.event_timestamp = event_timestamp;

                if current_timestamp - old_process_timestamp < self.period {
                    return true;
                }
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    use crate::runtime::worker::backpressure::{EventTimeBackpressure, RawBackpressure};
    use crate::utils::date_time::current_timestamp_millis;

    #[test]
    pub fn event_time_backpressure1_test() {
        let backpressure = Rc::new(RefCell::new(RawBackpressure::new()));
        let bp1 = backpressure.clone();
        let bp2 = backpressure.clone();

        bp1.borrow_mut()
            .register(EventTimeBackpressure::new(Duration::from_millis(1), 2));
        bp2.borrow_mut()
            .register(EventTimeBackpressure::new(Duration::from_millis(1), 2));
    }

    #[test]
    pub fn event_time_backpressure_test() {
        let period = Duration::from_millis(50);
        let mut backpressure = EventTimeBackpressure::new(period, 2);

        let begin_timestamp = current_timestamp_millis();

        let event_timestamp = begin_timestamp - 1000 * 60 * 60;
        let loop_times = 100;
        let step = 10;
        for n in 0..loop_times {
            let dur = backpressure.filter(event_timestamp + n * step);
            if let Some(dur) = dur {
                std::thread::sleep(dur);
            }
        }

        let end_timestamp = current_timestamp_millis();

        let pause_times = (end_timestamp - begin_timestamp) / (period / 2).as_millis() as u64;
        let expect_times = loop_times / step;

        println!("{}", pause_times);
        assert!(pause_times >= expect_times - 1);
        assert!(pause_times <= expect_times + 1);
    }
}
