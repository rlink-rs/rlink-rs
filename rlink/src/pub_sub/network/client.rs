use std::borrow::BorrowMut;
use std::collections::LinkedList;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::ReadHalf;
use tokio::net::TcpStream;
use tokio_util::codec::FramedRead;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::codec::{BytesCodec, FramedWrite};

use crate::api::element::Element;
use crate::api::properties::ChannelBaseOn;
use crate::api::runtime::{ChannelKey, TaskId};
use crate::channel::{
    bounded, named_channel_with_base, ElementReceiver, ElementSender, Receiver, Sender,
    TryRecvError, TrySendError,
};
use crate::metrics::{register_counter, Tag};
use crate::pub_sub::network::{ElementRequest, ElementResponse, ResponseCode};
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
    tcp_frame_max_size: u32,
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
            tcp_frame_max_size: 1024 * 1024,
        })
    }

    pub async fn send(&mut self) -> anyhow::Result<()> {
        info!(
            "Pull remote={}, local={}, channel_key={:?}",
            self.addr,
            self.stream.local_addr().unwrap(),
            self.channel_key,
        );

        let (read_half, write_half) = self.stream.split();
        let mut framed_write = FramedWrite::new(write_half, BytesCodec::new());

        let mut codec_framed: FramedRead<ReadHalf, LengthDelimitedCodec> =
            LengthDelimitedCodec::builder()
                .length_field_offset(0)
                .length_field_length(4)
                .length_adjustment(4) // 数据体截取位置，应和num_skip配置使用，保证frame要全部被读取
                .num_skip(0)
                .max_frame_length(self.tcp_frame_max_size as usize)
                .big_endian()
                .new_read(read_half);

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

        let mut batch_id = 0u16;
        let timeout = Duration::from_secs(6);
        loop {
            let request = ElementRequest {
                channel_key: self.channel_key.clone(),
                batch_pull_size: self.batch_pull_size,
                batch_id,
            };

            let (n, _) = batch_id.overflowing_add(1);
            batch_id = n;

            if is_enable_log() {
                info!("send request: {:?}", request);
            }

            let buffer: BytesMut = request.into();
            framed_write.send(buffer.freeze()).await?;

            let element_list = tokio::time::timeout(
                timeout,
                Self::recv_element(
                    codec_framed.borrow_mut(),
                    self.channel_key,
                    self.batch_pull_size,
                ),
            )
            .await??;

            let len = element_list.len();
            if len > 0 {
                for element in element_list {
                    match self.sender.try_send_opt(element) {
                        Some(t) => send_to_channel(&self.sender, t).await?,
                        None => {}
                    }
                }

                counter.fetch_add(len as u64, Ordering::Relaxed);
            }
            if len < 100 {
                async_sleep(Duration::from_secs(3)).await;
            }
        }
    }

    async fn recv_element(
        framed_read: &mut FramedRead<ReadHalf<'_>, LengthDelimitedCodec>,
        channel_key: ChannelKey,
        batch_size: u16,
    ) -> anyhow::Result<LinkedList<Element>> {
        if is_enable_log() {
            info!("begin loop recv elements. channel: {:?}", channel_key);
        }
        let mut element_list = LinkedList::new();
        for n in 0..batch_size + 1 {
            let message = framed_read
                .next()
                .await
                .ok_or(anyhow!("framed read nothing"))?;

            let bytes = message.map_err(|e| anyhow!("framed read error {}", e))?;

            let ElementResponse { code, element } = ElementResponse::try_from(bytes)?;

            match code {
                ResponseCode::Ok => {
                    let mut element = element.unwrap();
                    element.set_channel_key(channel_key);
                    element_list.push_back(element);
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
                    return Ok(element_list);
                }
                ResponseCode::Empty => {
                    if is_enable_log() {
                        info!(
                            "recv `Empty` code from remoting, total recv size {}, channel: {:?}",
                            n, channel_key
                        );
                    }

                    return Ok(element_list);
                }
                ResponseCode::NoService => {
                    return Err(anyhow!(
                        "remoting no service. unreachable! after an end flag, the client has close"
                    ));
                }
                _ => {
                    return Err(anyhow!("unrecognized remoting code {:?}", code));
                }
            }
        }

        // Err(anyhow!(
        //     "unreachable, should be interrupted at `Empty` or `BatchFinish` code"
        // ))
        unreachable!()
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
                error!("close client error. {}", e);
            }
        }
    }
}

async fn send_to_channel(sender: &ElementSender, element: Element) -> anyhow::Result<()> {
    let mut ele = element;
    let mut loops = 0;
    loop {
        ele = match sender.try_send(ele) {
            Ok(_) => {
                return Ok(());
            }
            Err(TrySendError::Full(ele)) => {
                // if is_enable_log() {
                error!("> net input channel block, channel: {:?}", ele);
                // }

                if loops == 60 {
                    error!("net input channel block and try with 60 times");
                    loops = 0;
                }
                loops += 1;

                async_sleep(Duration::from_secs(1)).await;
                error!("< net input channel block, channel: {:?}", ele);

                ele
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err(anyhow!("client network send to input channel Disconnected. unreachable! the next job channel must live longer than the client"));
            }
        }
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
