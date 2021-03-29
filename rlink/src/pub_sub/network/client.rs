use std::borrow::BorrowMut;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BytesMut};
use futures::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::ReadHalf;
use tokio::net::TcpStream;
use tokio_util::codec::FramedRead;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::codec::{BytesCodec, FramedWrite};

use crate::api::element::{Element, Serde};
use crate::api::properties::ChannelBaseOn;
use crate::api::runtime::{ChannelKey, TaskId};
use crate::channel::{
    bounded, named_channel_with_base, ElementReceiver, ElementSender, Receiver, Sender,
    TryRecvError, TrySendError,
};
use crate::metrics::{register_counter, Tag};
use crate::pub_sub::network::{ElementRequest, ResponseCode};
use crate::runtime::ClusterDescriptor;
use crate::utils::thread::{async_runtime_multi, async_sleep};

pub(crate) static ENABLE_LOG: AtomicBool = AtomicBool::new(false);

#[inline]
fn is_enable_log() -> bool {
    ENABLE_LOG.load(Ordering::Relaxed)
}

#[inline]
pub(crate) fn enable_log() {
    ENABLE_LOG.store(true, Ordering::Relaxed)
}

#[inline]
pub(crate) fn disable_log() {
    ENABLE_LOG.store(false, Ordering::Relaxed)
}

const BATCH_PULL_SIZE: u16 = 6000;

lazy_static! {
    static ref C: (
        Sender<(ChannelKey, ElementSender)>,
        Receiver<(ChannelKey, ElementSender)>
    ) = bounded(32);
}

pub(crate) fn subscribe(
    source_task_ids: &Vec<TaskId>,
    target_task_id: &TaskId,
    channel_size: usize,
    channel_base_on: ChannelBaseOn,
) -> ElementReceiver {
    let (sender, receiver) = named_channel_with_base(
        "NetworkSubscribe",
        vec![
            Tag::from(("source_job_id", source_task_ids[0].job_id.0)),
            Tag::from(("target_job_id", target_task_id.job_id.0)),
            Tag::from(("target_task_number", target_task_id.task_number)),
        ],
        channel_size,
        channel_base_on,
    );

    for source_task_id in source_task_ids {
        let sub_key = ChannelKey {
            source_task_id: source_task_id.clone(),
            target_task_id: target_task_id.clone(),
        };

        subscribe_post(sub_key, sender.clone());
    }

    receiver
}

fn subscribe_post(channel_key: ChannelKey, sender: ElementSender) {
    let c: &(
        Sender<(ChannelKey, ElementSender)>,
        Receiver<(ChannelKey, ElementSender)>,
    ) = &*C;
    c.0.send((channel_key, sender)).unwrap()
}

pub(crate) fn run_subscribe(cluster_descriptor: Arc<ClusterDescriptor>) {
    async_runtime_multi("client", 4).block_on(subscribe_listen(cluster_descriptor));
    info!("network subscribe task stop");
}

async fn subscribe_listen(cluster_descriptor: Arc<ClusterDescriptor>) {
    let c: &(
        Sender<(ChannelKey, ElementSender)>,
        Receiver<(ChannelKey, ElementSender)>,
    ) = &*C;

    let delay = Duration::from_millis(50);
    let mut idle_counter = 0usize;
    let mut join_handles = Vec::new();
    loop {
        match c.1.try_recv() {
            Ok((channel_key, sender)) => {
                let worker_manager_descriptor = cluster_descriptor
                    .get_worker_manager(&channel_key.source_task_id)
                    .expect("WorkerManagerDescriptor not found");
                let addr = SocketAddr::from_str(&worker_manager_descriptor.task_manager_address)
                    .expect("parse address error");

                let join_handle = tokio::spawn(async move {
                    loop_client_task(channel_key.clone(), sender, addr, BATCH_PULL_SIZE).await;
                    channel_key
                });
                join_handles.push(join_handle);

                idle_counter = 0;
            }
            Err(TryRecvError::Empty) => {
                idle_counter += 1;
                if idle_counter < 20 * 10 {
                    async_sleep(delay).await;
                } else {
                    // all task registration must be completed within 10 seconds
                    info!("subscribe listen task finish");
                    break;
                }
            }
            Err(TryRecvError::Disconnected) => {
                info!("subscribe_listen channel is Disconnected")
            }
        }
    }

    for join_handle in join_handles {
        match join_handle.await {
            Ok(channel_key) => info!("channel({:?}) network subscribe stop", channel_key),
            Err(e) => error!("Client task error. {}", e),
        }
    }
}

async fn loop_client_task(
    channel_key: ChannelKey,
    sender: ElementSender,
    addr: SocketAddr,
    batch_pull_size: u16,
) {
    loop {
        match client_task(channel_key, sender.clone(), addr, batch_pull_size).await {
            Ok(_) => {
                info!("client close({:?})", channel_key);
                break;
            }
            Err(e) => {
                error!("client({}) task error. {}", addr, e)
            }
        }

        async_sleep(Duration::from_secs(3)).await;
    }
}

async fn client_task(
    channel_key: ChannelKey,
    sender: ElementSender,
    addr: SocketAddr,
    batch_pull_size: u16,
) -> anyhow::Result<()> {
    let mut client = Client::new(channel_key, sender.clone(), addr, batch_pull_size).await?;
    let rt = client.send().await;
    client.close_rough().await;

    rt
}

pub(crate) struct Client {
    channel_key: ChannelKey,
    sender: ElementSender,

    pub(crate) addr: SocketAddr,
    batch_pull_size: u16,
    stream: TcpStream,
}

impl Client {
    pub async fn new(
        channel_key: ChannelKey,
        sender: ElementSender,
        addr: SocketAddr,
        batch_pull_size: u16,
    ) -> anyhow::Result<Self> {
        let std_stream = std::net::TcpStream::connect(addr)?;
        std_stream.set_nonblocking(true)?;
        std_stream.set_read_timeout(Some(Duration::from_secs(20)))?;
        std_stream.set_write_timeout(Some(Duration::from_secs(20)))?;

        let stream = TcpStream::from_std(std_stream)?;

        Ok(Client {
            channel_key,
            sender,
            addr,
            batch_pull_size,
            stream,
        })
    }

    pub async fn send(&mut self) -> anyhow::Result<()> {
        info!(
            "Pull remote={}, channel_key={:?}",
            self.addr, self.channel_key
        );

        let (r, w) = self.stream.split();
        let mut sink = FramedWrite::new(w, BytesCodec::new());

        let mut codec_framed: FramedRead<ReadHalf, LengthDelimitedCodec> =
            LengthDelimitedCodec::builder()
                .length_field_length(4)
                .new_read(r);

        let tags = vec![
            Tag::from(("source_job_id", self.channel_key.source_task_id.job_id.0)),
            Tag::from((
                "source_task_number",
                self.channel_key.source_task_id.task_number,
            )),
            Tag::from(("target_job_id", self.channel_key.target_task_id.job_id.0)),
            Tag::from((
                "target_task_number",
                self.channel_key.target_task_id.task_number,
            )),
        ];
        let counter = Arc::new(AtomicU64::new(0));
        register_counter("NetWorkClient", tags, counter.clone());

        loop {
            let request = ElementRequest {
                channel_key: self.channel_key.clone(),
                batch_pull_size: self.batch_pull_size,
            };

            let buffer: BytesMut = request.into();
            sink.send(buffer.freeze()).await?;

            let can_continue = Self::recv_element(
                codec_framed.borrow_mut(),
                &counter,
                &self.sender,
                self.channel_key,
                self.batch_pull_size,
            )
            .await?;
            if !can_continue {
                return Ok(());
            }
        }
    }

    async fn recv_element(
        codec_framed: &mut FramedRead<ReadHalf<'_>, LengthDelimitedCodec>,
        counter: &Arc<AtomicU64>,
        sender: &ElementSender,
        channel_key: ChannelKey,
        batch_size: u16,
    ) -> anyhow::Result<bool> {
        for n in 0..batch_size + 1 {
            let message = codec_framed
                .next()
                .await
                .ok_or(anyhow!("framed read nothing"))?;
            let bytes = message.map_err(|e| anyhow!("framed read error {}", e))?;

            let (response_code, element) = frame_parse(bytes);
            match response_code {
                ResponseCode::Ok => {
                    let mut end = false;
                    let mut element = element.unwrap();
                    match element.borrow_mut() {
                        Element::Record(record) => record.channel_key = channel_key,
                        Element::Watermark(watermark) => {
                            watermark.channel_key = channel_key;
                            debug!("client recv Watermark {}", watermark.timestamp);
                        }
                        Element::StreamStatus(stream_status) => {
                            stream_status.channel_key = channel_key;
                            end = stream_status.end;
                        }
                        _ => {}
                    }

                    Self::send_to_channel(element, sender).await?;
                    counter.fetch_add(1, Ordering::Relaxed);

                    if end {
                        info!("client recv an end flag and unsubscribe");
                        return Ok(false);
                    }
                }
                ResponseCode::BatchFinish => {
                    if n != batch_size {
                        error!(
                            "inconsistent response and request, channel: {:?}",
                            channel_key
                        );
                    }
                    if is_enable_log() {
                        info!("batch finish, channel: {:?}", channel_key);
                    }
                    return Ok(true);
                }
                ResponseCode::Empty => {
                    async_sleep(Duration::from_secs(1)).await;
                    if is_enable_log() {
                        info!(
                            "recv `Empty` code from remoting, total recv size {}, channel: {:?}",
                            n, channel_key
                        );
                    }

                    return Ok(true);
                }
                ResponseCode::NoService => {
                    warn!(
                        "remoting no service. unreachable! after an end flag, the client has close"
                    );
                    return Ok(false);
                }
                _ => {
                    return Err(anyhow!("unrecognized remoting code {:?}", response_code));
                }
            }
        }

        // Err(anyhow!(
        //     "unreachable, should be interrupted at `Empty` or `BatchFinish` code"
        // ))
        unreachable!()
    }

    async fn send_to_channel(element: Element, sender: &ElementSender) -> anyhow::Result<()> {
        let mut ele = element;
        let mut loops = 0;
        loop {
            ele = match sender.try_send(ele) {
                Ok(_) => {
                    return Ok(());
                }
                Err(TrySendError::Full(ele)) => {
                    if is_enable_log() {
                        error!("net input channel block, channel: {:?}", ele);
                    }

                    if loops == 60 {
                        error!("net input channel block and try with 60 times");
                        loops = 0;
                    }
                    loops += 1;

                    async_sleep(Duration::from_secs(1)).await;
                    ele
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err(anyhow!("client network send to input channel Disconnected. unreachable! the next job channel must live longer than the client"));
                }
            }
        }
    }

    // maybe lost data in send/recv buffer
    #[allow(dead_code)]
    pub async fn close(mut self) -> std::io::Result<()> {
        self.stream.shutdown().await
    }

    #[allow(dead_code)]
    pub async fn close_rough(self) {
        match self.close().await {
            Ok(_) => {}
            Err(e) => {
                error!("close client error {}", e);
            }
        }
    }
}

fn frame_parse(mut data: BytesMut) -> (ResponseCode, Option<Element>) {
    data.advance(4); // skip header length
    let code = data.get_u8();
    let code = ResponseCode::from(code);
    if code == ResponseCode::Ok {
        (code, Some(Element::deserialize(data.borrow_mut())))
    } else {
        (code, None)
    }
}

#[cfg(test)]
mod tests {
    use crate::api::element::Element;
    use crate::api::runtime::{ChannelKey, JobId, TaskId};
    use crate::channel::named_channel;
    use crate::pub_sub::network::client::Client;

    #[tokio::test]
    pub async fn client_test() {
        let channel_key = ChannelKey {
            source_task_id: TaskId {
                job_id: JobId(0),
                task_number: 5,
                num_tasks: 30,
            },
            target_task_id: TaskId {
                job_id: JobId(4),
                task_number: 14,
                num_tasks: 30,
            },
        };
        let (sender, receiver) = named_channel::<Element>("test", vec![], 10000);
        std::thread::spawn(move || {
            while let Ok(v) = receiver.recv() {
                println!("{:?}", v);
            }
        });

        let addr = "10.100.189.45:28820".parse().unwrap();

        let mut client = Client::new(channel_key, sender, addr, 100).await.unwrap();
        client.send().await.unwrap();
        client.close().await.unwrap();
    }
}
