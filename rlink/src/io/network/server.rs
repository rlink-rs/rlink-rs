use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use futures::{SinkExt, StreamExt};
use rand::prelude::*;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio_util::codec::LengthDelimitedCodec;

use crate::api::element::{Element, Serde};
use crate::channel::ElementReceiver;
use crate::net::ResponseCode;
use crate::utils::get_runtime;

#[derive(Debug, Clone)]
pub(crate) struct Server {
    ip: String,
    bind_addr: Arc<RwLock<Option<SocketAddr>>>,
    tcp_frame_max_size: u32,
    chain_receivers: Arc<RwLock<HashMap<u32, Vec<ElementReceiver>>>>,
}

impl Server {
    pub fn new(ip: String) -> Self {
        Server {
            ip,
            bind_addr: Arc::new(RwLock::new(None)),
            tcp_frame_max_size: 1024 * 1024,
            chain_receivers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn add_receivers_sync(&mut self, receivers: HashMap<u32, Vec<ElementReceiver>>) {
        let mut self_clone = self.clone();
        get_runtime().block_on(self_clone.add_receivers(receivers));
    }

    pub async fn add_receivers(&mut self, receivers: HashMap<u32, Vec<ElementReceiver>>) {
        let mut chain_receivers = self.chain_receivers.write().await;
        *chain_receivers = receivers;
    }

    pub fn get_bind_addr_sync(&self) -> Option<SocketAddr> {
        let self_clone = self.clone();
        get_runtime().block_on(self_clone.get_bind_addr())
    }

    pub async fn get_bind_addr(&self) -> Option<SocketAddr> {
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

    pub async fn session_accept(self, mut listener: TcpListener) -> std::io::Result<()> {
        loop {
            let (socket, remote_addr) = listener.accept().await?;
            info!(
                "Socket accepted and created connection. remote addr: {}",
                self.sock_addr_to_str(&remote_addr)
            );

            socket.set_keepalive(Option::Some(Duration::from_secs(120)))?;

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

                    let (job_id, target_job_id, target_task_number, batch_size) =
                        self.frame_parse(bytes, remote_addr.ip().to_string());
                    let code = ResponseCode::Ok;
                    match self
                        .response(
                            code,
                            job_id,
                            target_task_number,
                            batch_size,
                            codec_framed.get_mut(),
                        )
                        .await
                    {
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

    async fn response(
        &self,
        response_code: ResponseCode,
        chain_id: u32,
        partition_num: u16,
        batch_size: u16,
        framed_read: &mut tokio::net::TcpStream,
    ) -> Result<(), std::io::Error> {
        if response_code == ResponseCode::ParseErr {
            self.send(response_code, None, framed_read).await
        } else {
            let chain_receivers = self.chain_receivers.read().await;
            match chain_receivers.get(&chain_id) {
                Some(receivers) => {
                    let receiver: &ElementReceiver = receivers.get(partition_num as usize).unwrap();
                    for _ in 0..batch_size {
                        match receiver.try_recv() {
                            Ok(element) => {
                                self.send(ResponseCode::Ok, Some(element), framed_read)
                                    .await?
                            }
                            Err(e) => {
                                debug!("try recv(partition_num={}) row: {}", partition_num, e);
                                return self.send(ResponseCode::Empty, None, framed_read).await;
                            }
                        }
                    }
                    self.send(ResponseCode::BatchFinish, None, framed_read)
                        .await
                }
                None => {
                    error!("chain_id({}) not found", chain_id);
                    self.send(ResponseCode::ReadErr, None, framed_read).await
                }
            }
        }
    }

    async fn send(
        &self,
        code: ResponseCode,
        // data: &[u8],
        element: Option<Element>,
        framed_read: &mut tokio::net::TcpStream,
    ) -> Result<(), std::io::Error> {
        let mut req = match element {
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

        codec_framed0.send(req.to_bytes()).await
    }

    /// Return the `partition_num`
    fn frame_parse(&self, mut data: BytesMut, _peer_addr: String) -> (u32, u32, u16, u16) {
        data.advance(4); // skip header length
        let job_id = data.get_u32();
        let target_job_id = data.get_u32();
        let target_task_number = data.get_u16();
        let batch_pull_size = data.get_u16();
        (job_id, target_job_id, target_task_number, batch_pull_size)
    }

    fn sock_addr_to_str(&self, addr: &std::net::SocketAddr) -> String {
        format!("{}:{}", addr.ip().to_string(), addr.port())
    }
}
