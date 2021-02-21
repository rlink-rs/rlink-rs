use std::borrow::BorrowMut;
use std::convert::TryFrom;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use rand::prelude::*;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio_util::codec::LengthDelimitedCodec;

use crate::api::element::{Element, Serde};
use crate::api::properties::ChannelBaseOn;
use crate::api::runtime::{ChannelKey, TaskId};
use crate::channel::{named_channel_with_base, ElementReceiver, ElementSender, TryRecvError};
use crate::metrics::Tag;
use crate::pub_sub::network::{ElementRequest, ResponseCode};
use crate::utils::thread::get_runtime;

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
        get_runtime().block_on(self_clone.bind_addr())
    }

    pub async fn bind_addr(&self) -> Option<SocketAddr> {
        let addr = self.bind_addr.read().await;
        let addr = *addr;
        addr.clone()
    }

    pub fn serve_sync(&self) -> std::io::Result<()> {
        let self_clone = self.clone();
        get_runtime().block_on(self_clone.serve())
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
            let port = rng.gen_range(10000, 30000);
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
        let mut codec_framed = LengthDelimitedCodec::builder()
            .length_field_offset(0)
            .length_field_length(4)
            .length_adjustment(4) // 数据体截取位置，应和num_skip配置使用，保证frame要全部被读取
            .num_skip(0)
            .max_frame_length(self.tcp_frame_max_size as usize)
            .big_endian()
            .new_read(socket);

        while let Some(message) = codec_framed.next().await {
            match message {
                Ok(bytes) => {
                    if log_enabled!(log::Level::Debug) {
                        debug!("tcp bytes: {:?}", bytes);
                    }

                    match self.subscribe_handle(bytes, codec_framed.get_mut()).await {
                        Ok(_) => {}
                        Err(err) => {
                            error!(
                                "Socket response with error. remote addr: {}, error: {:?}",
                                self.sock_addr_to_str(&remote_addr),
                                err
                            );
                            return;
                        }
                    }
                }
                Err(err) => {
                    error!(
                        "Socket closed with error. remote addr: {}, error: {:?}",
                        self.sock_addr_to_str(&remote_addr),
                        err
                    );
                    return;
                }
            }
        }
        info!(
            "Socket received FIN packet and closed connection. remote addr: {}",
            self.sock_addr_to_str(&remote_addr)
        );
    }

    async fn subscribe_handle(
        &self,
        bytes: BytesMut,
        framed_read: &mut tokio::net::TcpStream,
    ) -> Result<(), std::io::Error> {
        match ElementRequest::try_from(bytes) {
            Ok(request) => {
                let ElementRequest {
                    channel_key,
                    batch_pull_size,
                } = request;

                match get_network_channel(&channel_key) {
                    Some(receiver) => {
                        for _ in 0..batch_pull_size {
                            match receiver.try_recv() {
                                Ok(element) => {
                                    self.send(ResponseCode::Ok, Some(element), framed_read)
                                        .await?
                                }
                                Err(TryRecvError::Empty) => {
                                    debug!("try recv channel_key({:?}) empty", channel_key);
                                    return self.send(ResponseCode::Empty, None, framed_read).await;
                                }
                                Err(TryRecvError::Disconnected) => {
                                    panic!(format!("channel_key({:?}) close", channel_key))
                                }
                            }
                        }
                        self.send(ResponseCode::BatchFinish, None, framed_read)
                            .await
                    }
                    None => {
                        warn!(
                            "channel_key({:?}) not found, maybe the job haven't initialized yet",
                            channel_key
                        );
                        self.send(ResponseCode::Empty, None, framed_read).await
                    }
                }
            }
            Err(e) => {
                error!("parse request error. {}", e);
                self.send(ResponseCode::ParseErr, None, framed_read).await
            }
        }
    }

    async fn send(
        &self,
        code: ResponseCode,
        element: Option<Element>,
        framed_read: &mut tokio::net::TcpStream,
    ) -> Result<(), std::io::Error> {
        let req = match element {
            Some(element) => {
                let element_len = element.capacity();
                let mut req = bytes::BytesMut::with_capacity(4 + 1 + element_len);
                req.put_u32(element_len as u32 + 1); // (code + body).length
                req.put_u8(code as u8);

                element.serialize(req.borrow_mut());
                req
            }
            None => {
                let mut req = bytes::BytesMut::with_capacity(4 + 1);
                req.put_u32(1); // (code + body).length
                req.put_u8(code as u8);
                req
            }
        };

        let mut codec_framed0 = LengthDelimitedCodec::builder()
            .length_field_length(4)
            .new_framed(framed_read);

        codec_framed0.send(req.freeze()).await
    }

    fn sock_addr_to_str(&self, addr: &std::net::SocketAddr) -> String {
        format!("{}:{}", addr.ip().to_string(), addr.port())
    }
}
