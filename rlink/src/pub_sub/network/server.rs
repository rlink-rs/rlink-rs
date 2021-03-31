use std::borrow::BorrowMut;
use std::collections::LinkedList;
use std::convert::TryFrom;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use rand::prelude::*;
use tokio::net::tcp::WriteHalf;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio_util::codec::{BytesCodec, FramedWrite, LengthDelimitedCodec};

use crate::api::element::Element;
use crate::api::properties::ChannelBaseOn;
use crate::api::runtime::{ChannelKey, TaskId};
use crate::channel::{named_channel_with_base, ElementReceiver, ElementSender, TryRecvError};
use crate::metrics::Tag;
use crate::pub_sub::network::{ElementRequest, ElementResponse, ResponseCode};
use crate::utils::thread::{async_runtime, async_runtime_single};

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

lazy_static! {
    static ref NETWORK_CHANNELS: DashMap<ChannelKey, ElementReceiver> = DashMap::new();
}

pub(crate) fn publish(
    source_task_id: &TaskId,
    target_task_ids: &Vec<TaskId>,
    channel_size: usize,
    channel_base_on: ChannelBaseOn,
) -> Vec<(ChannelKey, ElementSender)> {
    let mut senders = Vec::new();
    for target_task_id in target_task_ids {
        let channel_key = ChannelKey {
            source_task_id: source_task_id.clone(),
            target_task_id: target_task_id.clone(),
        };

        let (sender, receiver) = named_channel_with_base(
            "NetworkPublish",
            vec![
                Tag::from(("source_job_id", source_task_id.job_id.0)),
                Tag::from(("source_task_number", source_task_id.task_number)),
                Tag::from(("target_job_id", target_task_id.job_id.0)),
                Tag::from(("target_task_number", target_task_id.task_number)),
            ],
            channel_size,
            channel_base_on,
        );

        senders.push((channel_key.clone(), sender));
        set_network_channel(channel_key, receiver);
    }

    senders
}

fn set_network_channel(key: ChannelKey, receiver: ElementReceiver) {
    let network_channels: &DashMap<ChannelKey, ElementReceiver> = &*NETWORK_CHANNELS;
    if network_channels.contains_key(&key) {
        // maybe dag build bug
        panic!("network ChannelKey register must unique");
    }
    network_channels.insert(key, receiver);
}

fn get_network_channel(key: &ChannelKey) -> Option<ElementReceiver> {
    let network_channels: &DashMap<ChannelKey, ElementReceiver> = &*NETWORK_CHANNELS;
    network_channels.get(key).map(|x| x.value().clone())
}

#[derive(Debug, Clone)]
pub(crate) struct Server {
    ip: String,
    bind_addr: Arc<RwLock<Option<SocketAddr>>>,
    tcp_frame_max_size: u32,
}

impl Server {
    pub fn new(ip: String) -> Self {
        Server {
            ip,
            bind_addr: Arc::new(RwLock::new(None)),
            tcp_frame_max_size: 1024 * 1024,
        }
    }

    pub fn bind_addr_sync(&self) -> Option<SocketAddr> {
        let self_clone = self.clone();
        async_runtime_single().block_on(self_clone.bind_addr())
    }

    pub async fn bind_addr(&self) -> Option<SocketAddr> {
        let addr = self.bind_addr.read().await;
        let addr = *addr;
        addr.clone()
    }

    pub fn serve_sync(&self) -> std::io::Result<()> {
        let self_clone = self.clone();
        async_runtime("server").block_on(self_clone.serve())
    }

    pub async fn serve(&self) -> std::io::Result<()> {
        let listener = self.try_bind(self.ip.as_str()).await?;

        let addr: SocketAddr = listener.local_addr().unwrap();

        let ip = IpAddr::from_str(self.ip.as_str()).expect("parse ip error");
        let serve_addr = SocketAddr::new(ip, addr.port());

        // async/await Lock must be a code block
        {
            let mut inuse_port = self.bind_addr.write().await;
            *inuse_port = Some(serve_addr.clone());

            info!(
                "tcp server listening on: {}, publish address: {}",
                (*inuse_port).clone().unwrap().to_string(),
                serve_addr
            );
        }

        self.clone().session_accept(listener).await
    }

    pub async fn try_bind(&self, _ip: &str) -> Result<TcpListener, std::io::Error> {
        let mut rng = rand::thread_rng();
        let loops = 30;
        for index in 0..loops {
            let port = rng.gen_range(10000..30000);
            let address = format!("0.0.0.0:{}", port);

            match TcpListener::bind(&address).await {
                Ok(listener) => return Ok(listener),
                Err(e) => {
                    if index == loops - 1 {
                        return Err(e);
                    }
                    warn!("try bind port={} error {}", port, e);
                }
            }
        }

        panic!("port inuse");
    }

    pub async fn session_accept(self, listener: TcpListener) -> std::io::Result<()> {
        loop {
            let (socket, remote_addr) = listener.accept().await?;
            info!(
                "Socket accepted and created connection. remote addr: {}",
                self.sock_addr_to_str(&remote_addr)
            );

            // socket.set_keepalive(Option::Some(Duration::from_secs(120)))?;

            tokio::spawn(self.clone().session_process(socket, remote_addr));
        }
    }

    async fn session_process(self, socket: TcpStream, remote_addr: SocketAddr) {
        match self.session_process0(socket).await {
            Ok(_) => {}
            Err(e) => {
                error!(
                    "session process error, remote address: {}. {}",
                    self.sock_addr_to_str(&remote_addr),
                    e
                );
            }
        }
    }

    async fn session_process0(&self, mut socket: TcpStream) -> anyhow::Result<()> {
        let (read_half, write_half) = socket.split();
        let mut framed_read = LengthDelimitedCodec::builder()
            .length_field_offset(0)
            .length_field_length(4)
            .length_adjustment(4) // 数据体截取位置，应和num_skip配置使用，保证frame要全部被读取
            .num_skip(0)
            .max_frame_length(self.tcp_frame_max_size as usize)
            .big_endian()
            .new_read(read_half);
        let mut framed_write = FramedWrite::new(write_half, BytesCodec::new());

        while let Some(message) = framed_read.next().await {
            match message {
                Ok(bytes) => {
                    if log_enabled!(log::Level::Debug) {
                        debug!("tcp bytes: {:?}", bytes);
                    }
                    let request = ElementRequest::try_from(bytes)?;
                    self.subscribe_handle(request, framed_write.borrow_mut())
                        .await?;
                }
                Err(e) => {
                    return Err(anyhow!("Socket closed with error. {}", e));
                }
            }
        }
        Err(anyhow!("Socket received FIN packet and closed connection",))
    }

    async fn subscribe_handle(
        &self,
        request: ElementRequest,
        framed_write: &mut FramedWrite<WriteHalf<'_>, BytesCodec>,
    ) -> Result<(), std::io::Error> {
        if is_enable_log() {
            info!("recv request: {:?}", request);
        }
        let ElementRequest {
            channel_key,
            batch_pull_size,
            batch_id: _,
        } = request;

        let element_list = self.batch_get(&channel_key, batch_pull_size);
        let len = element_list.len();
        for element in element_list {
            self.send(ElementResponse::ok(element), framed_write)
                .await?
        }

        let end_response = if len == batch_pull_size as usize {
            ElementResponse::end(ResponseCode::BatchFinish)
        } else {
            ElementResponse::end(ResponseCode::Empty)
        };

        self.send(end_response, framed_write).await?;

        if is_enable_log() {
            info!(
                "try recv empty, total recv size {}, channel_key: {:?}",
                len, channel_key
            );
        }

        Ok(())
    }

    fn batch_get(&self, channel_key: &ChannelKey, batch_pull_size: u16) -> LinkedList<Element> {
        let mut element_list = LinkedList::new();
        match get_network_channel(&channel_key) {
            Some(receiver) => {
                for _ in 0..batch_pull_size {
                    match receiver.try_recv() {
                        Ok(element) => {
                            element_list.push_back(element);
                        }
                        Err(TryRecvError::Empty) => {
                            break;
                        }
                        Err(TryRecvError::Disconnected) => {
                            // panic!(format!("channel_key({:?}) close", channel_key))
                            info!("channel_key({:?}) close", channel_key);
                            break;
                        }
                    }
                }
            }
            None => {
                warn!(
                    "channel_key({:?}) not found, maybe the job haven't initialized yet",
                    channel_key
                );
            }
        }

        element_list
    }

    async fn send(
        &self,
        response: ElementResponse,
        framed_write: &mut FramedWrite<WriteHalf<'_>, BytesCodec>,
    ) -> Result<(), std::io::Error> {
        let req: BytesMut = response.into();
        framed_write.send(req.freeze()).await
    }

    fn sock_addr_to_str(&self, addr: &std::net::SocketAddr) -> String {
        format!("{}:{}", addr.ip().to_string(), addr.port())
    }
}
