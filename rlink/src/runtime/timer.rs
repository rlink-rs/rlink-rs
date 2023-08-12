use futures::Stream;
use std::borrow::BorrowMut;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::time::{interval_at, Duration, Instant};

use crate::channel::receiver::ChannelReceiver;
use crate::channel::sender::ChannelSender;
use crate::channel::{named_channel, TryRecvError};
use crate::core::element::Element;
use crate::core::runtime::CheckpointId;
use crate::utils;

#[derive(Clone)]
struct TimerSender {
    name: String,
    sender: ChannelSender<u64>,
    interval: Duration,
    front_window: u64,
}

impl TimerSender {
    pub fn new(name: &str, interval: Duration, sender: ChannelSender<u64>) -> Self {
        Self {
            name: name.to_string(),
            sender,
            interval,
            front_window: 0,
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }
}

pub struct TimerChannel {
    receiver: ChannelReceiver<u64>,
}

impl TimerChannel {
    pub fn new(receiver: ChannelReceiver<u64>) -> Self {
        TimerChannel { receiver }
    }

    #[allow(unused)]
    pub async fn recv(&mut self) -> Option<u64> {
        self.receiver.recv().await
    }
}

impl Stream for TimerChannel {
    type Item = u64;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

#[derive(Clone)]
pub struct WindowTimer {
    sender: ChannelSender<TimerSender>,
}

impl Debug for WindowTimer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("WindowTimer").finish()
    }
}

impl WindowTimer {
    fn new(sender: ChannelSender<TimerSender>) -> Self {
        WindowTimer { sender }
    }

    pub fn register(&self, name: &str, interval: Duration) -> anyhow::Result<TimerChannel> {
        info!("begin register channel: {}", interval.as_millis());

        let (sender, receiver) = named_channel("TimerChannelNotify", vec![], 1);
        let timer_sender = TimerSender::new(name, interval, sender);
        self.sender
            .try_send(timer_sender)
            .map_err(|_e| anyhow!("register TimerSender error"))?;

        let timer_channel = TimerChannel::new(receiver);
        Ok(timer_channel)
    }
}

pub async fn start_window_timer() -> WindowTimer {
    let (sender, mut receiver): (ChannelSender<TimerSender>, ChannelReceiver<TimerSender>) =
        named_channel("WindowTimerRegister", vec![], 1000);

    tokio::spawn(async move {
        let start = Instant::now() + Duration::from_secs(2);
        let mut interval = interval_at(start, Duration::from_secs(2));

        let mut timer_senders: Vec<TimerSender> = Vec::new();
        loop {
            // delay first
            interval.tick().await;

            loop {
                match receiver.try_recv() {
                    Ok(timer_sender) => {
                        info!(
                            "register {}ms window timer [{}], start scheduling",
                            &timer_sender.interval().as_millis(),
                            timer_sender.name(),
                        );
                        timer_senders.push(timer_sender)
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        info!("window timer channel disconnected");
                        return;
                    }
                }
            }

            check_window(timer_senders.borrow_mut());
        }
    });

    WindowTimer::new(sender)
}

fn get_window_start_with_offset(timestamp: u64, window_size: u64) -> u64 {
    let timestamp = timestamp as i64;
    let window_size = window_size as i64;
    (timestamp - (timestamp + window_size) % window_size) as u64
}

fn check_window(timer_senders: &mut Vec<TimerSender>) {
    let mut full_errs = 0;
    let ts = utils::date_time::current_timestamp_millis();
    for timer_channel in timer_senders {
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

pub struct BarrierStream {
    barrier_timer: TimerChannel,
}

impl BarrierStream {
    pub fn new(barrier_timer: TimerChannel) -> Self {
        Self { barrier_timer }
    }
}

impl Stream for BarrierStream {
    type Item = Element;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.barrier_timer).poll_next(cx) {
            Poll::Ready(window_time) => Poll::Ready(
                window_time.map(|window_time| Element::new_barrier(CheckpointId(window_time))),
            ),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct StreamStatusStream {
    stream_status_timer: TimerChannel,
}

impl StreamStatusStream {
    pub fn new(stream_status_timer: TimerChannel) -> Self {
        Self {
            stream_status_timer,
        }
    }
}

impl Stream for StreamStatusStream {
    type Item = Element;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream_status_timer).poll_next(cx) {
            Poll::Ready(window_time) => Poll::Ready(
                window_time.map(|window_time| Element::new_stream_status(window_time, false)),
            ),
            Poll::Pending => Poll::Pending,
        }
    }
}
