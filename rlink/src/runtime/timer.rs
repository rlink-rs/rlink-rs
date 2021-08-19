use std::borrow::BorrowMut;

use tokio::time::{interval_at, Duration, Instant};

use crate::channel::receiver::ChannelReceiver;
use crate::channel::sender::ChannelSender;
use crate::channel::{named_channel, RecvError, TryRecvError, TrySendError};
use crate::utils;

#[derive(Clone)]
pub struct TimerChannel {
    name: String,
    sender: ChannelSender<u64>,
    receiver: ChannelReceiver<u64>,
    interval: Duration,
    front_window: u64,
}

impl TimerChannel {
    pub fn new(name: &str, interval: Duration) -> Self {
        let (sender, receiver) = named_channel("TimerChannelNotify", vec![], 1);
        TimerChannel {
            name: name.to_string(),
            sender,
            receiver,
            interval,
            front_window: 0,
        }
    }

    pub fn _try_recv(&self) -> Result<u64, TryRecvError> {
        self.receiver.try_recv()
    }

    pub fn recv(&self) -> Result<u64, RecvError> {
        self.receiver.recv()
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

#[derive(Clone)]
pub struct WindowTimer {
    sender: ChannelSender<TimerChannel>,
}

impl WindowTimer {
    pub fn new(sender: ChannelSender<TimerChannel>) -> Self {
        WindowTimer { sender }
    }

    pub fn register(
        &self,
        name: &str,
        interval: Duration,
    ) -> Result<TimerChannel, TrySendError<TimerChannel>> {
        info!("begin register channel: {}", interval.as_millis());
        let timer_channel = TimerChannel::new(name, interval);
        self.sender
            .try_send(timer_channel.clone())
            .map(|_| timer_channel)
    }
}

pub fn start_window_timer() -> WindowTimer {
    let (sender, receiver): (ChannelSender<TimerChannel>, ChannelReceiver<TimerChannel>) =
        named_channel("WindowTimerRegister", vec![], 1000);

    utils::thread::spawn("window-timer", move || {
        utils::thread::async_runtime_single().block_on(async move {
            let start = Instant::now() + Duration::from_secs(2);
            let mut interval = interval_at(start, Duration::from_secs(2));

            let mut timer_channels: Vec<TimerChannel> = Vec::new();
            loop {
                // delay first
                interval.tick().await;

                loop {
                    match receiver.try_recv() {
                        Ok(timer_channel) => {
                            info!(
                                "register {}ms window timer [{}], start scheduling",
                                &timer_channel.interval().as_millis(),
                                timer_channel.name(),
                            );
                            timer_channels.push(timer_channel)
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            info!("window timer channel disconnected");
                            return;
                        }
                    }
                }

                check_window(timer_channels.borrow_mut());
            }
        });
    });

    WindowTimer::new(sender)
}

fn get_window_start_with_offset(timestamp: u64, window_size: u64) -> u64 {
    let timestamp = timestamp as i64;
    let window_size = window_size as i64;
    (timestamp - (timestamp + window_size) % window_size) as u64
}

fn check_window(timer_channels: &mut Vec<TimerChannel>) {
    let mut full_errs = 0;
    let ts = utils::date_time::current_timestamp_millis();
    for timer_channel in timer_channels {
        let window_start =
            get_window_start_with_offset(ts, timer_channel.interval.as_millis() as u64);
        if window_start != timer_channel.front_window {
            timer_channel.front_window = window_start;

            match timer_channel.sender.try_send(window_start) {
                Ok(_) => {
                    if full_errs > 0 {
                        info!(
                            "TimerChannel(interval={}ms) has resumed to normal",
                            timer_channel.interval.as_millis(),
                        )
                    }
                    full_errs = 0;
                }
                Err(e) => {
                    if full_errs == 0 {
                        error!(
                            "TimerChannel(interval={}ms) is full. {}",
                            timer_channel.interval.as_millis(),
                            e
                        )
                    }
                    full_errs += 1;
                }
            }
        }
    }
}
