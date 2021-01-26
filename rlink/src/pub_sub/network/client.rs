use std::borrow::BorrowMut;
use std::net::{Shutdown, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BytesMut};
use futures_util::sink::SinkExt;
use tokio::net::tcp::ReadHalf;
use tokio::net::TcpStream;
use tokio::stream::StreamExt;
use tokio_util::codec::FramedRead;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::codec::{BytesCodec, FramedWrite};

use crate::api::element::{Element, Serde};
use crate::api::runtime::{ChannelKey, TaskId};
use crate::channel::{
    bounded, named_channel, ElementReceiver, ElementSender, Receiver, Sender, TryRecvError,
    TrySendError,
};
use crate::metrics::{register_counter, Tag};
use crate::pub_sub::network::{ElementRequest, ResponseCode};
use crate::runtime::ApplicationDescriptor;
use crate::utils::thread::get_runtime;

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
) -> ElementReceiver {
    let (sender, receiver) = named_channel(
        "NetworkSubscribe",
        vec![
            Tag::new(
                "source_job_id".to_string(),
                source_task_ids[0].job_id.0.to_string(),
            ),
            Tag::new(
                "target_job_id".to_string(),
                target_task_id.job_id.0.to_string(),
            ),
            Tag::new(
                "target_task_number".to_string(),
                target_task_id.task_number.to_string(),
            ),
        ],
        channel_size,
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

pub(crate) fn run_subscribe(application_descriptor: ApplicationDescriptor) {
    get_runtime().block_on(subscribe_listen(application_descriptor));
}

async fn subscribe_listen(application_descriptor: ApplicationDescriptor) {
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
                let worker_manager_descriptor = application_descriptor
                    .get_worker_manager(&channel_key.source_task_id)
                    .expect("WorkerManagerDescriptor not found");
                let addr = SocketAddr::from_str(&worker_manager_descriptor.task_manager_address)
                    .expect("parse address error");

                let join_handle = tokio::spawn(async move {
                    let mut client = Client::new(channel_key, sender, addr, BATCH_PULL_SIZE)
                        .await
                        .unwrap();
                    client.send().await.unwrap();
                });
                join_handles.push(join_handle);

                idle_counter = 0;
            }
            Err(TryRecvError::Empty) => {
                idle_counter += 1;
                if idle_counter < 20 * 120 {
                    tokio::time::delay_for(delay).await;
                } else {
                    // all task registration must be completed within 2 minute
                    info!("subscribe listen task finish");
                    break;
                }
            }
            Err(TryRecvError::Disconnected) => panic!("channel is Disconnected"),
        }
    }

    for join_handle in join_handles {
        match join_handle.await {
            Ok(_) => {}
            Err(e) => error!("Client task error. {}", e),
        }
    }
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
        match TcpStream::connect(addr).await {
            Ok(stream) => Ok(Client {
                channel_key,
                sender,
                addr,
                batch_pull_size,
                stream,
            }),
            Err(e) => Err(anyhow::Error::new(e)),
        }
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
            Tag(
                "source_job_id".to_string(),
                self.channel_key.source_task_id.job_id.0.to_string(),
            ),
            Tag(
                "source_task_number".to_string(),
                self.channel_key.source_task_id.task_number.to_string(),
            ),
            Tag(
                "target_job_id".to_string(),
                self.channel_key.target_task_id.job_id.0.to_string(),
            ),
            Tag(
                "target_task_number".to_string(),
                self.channel_key.target_task_id.task_number.to_string(),
            ),
        ];
        let counter = Arc::new(AtomicU64::new(0));
        register_counter("NetWorkClient", tags, counter.clone());

        loop {
            let request = ElementRequest {
                channel_key: self.channel_key.clone(),
                batch_pull_size: self.batch_pull_size,
            };

            let mut buffer: BytesMut = request.into();
            sink.send(buffer.to_bytes()).await?;

            loop {
                match codec_framed.next().await {
                    Some(message) => match message {
                        Ok(bytes) => {
                            let (code, element) = frame_parse(bytes);
                            match code {
                                ResponseCode::Ok => {
                                    let mut element = element.unwrap();
                                    match element.borrow_mut() {
                                        Element::Record(record) => {
                                            record.channel_key = self.channel_key
                                        }
                                        Element::Watermark(watermark) => {
                                            debug!("net recv Watermark {}", watermark.timestamp);
                                        }
                                        _ => {}
                                    }

                                    Client::send_to_channel(element, &self.sender, &counter).await;
                                }
                                ResponseCode::BatchFinish => {
                                    // info!("batch finish");
                                    break;
                                }
                                ResponseCode::Empty => {
                                    tokio::time::delay_for(Duration::from_millis(1000)).await;
                                    debug!("No rows in remote");

                                    break;
                                }
                                _ => {
                                    return Err(anyhow::Error::msg(format!(
                                        "Unrecognized remote code {:?}",
                                        code
                                    )));
                                }
                            }
                        }
                        Err(e) => {
                            return Err(anyhow::Error::msg(format!("framed read error {}", e)));
                        }
                    },
                    None => {
                        return Err(anyhow::Error::msg("framed read nothing"));
                    }
                }
            }
        }
    }

    async fn send_to_channel(element: Element, sender: &ElementSender, counter: &Arc<AtomicU64>) {
        let mut ele = element;
        let mut loops = 0;
        loop {
            ele = match sender.try_send(ele) {
                Ok(_) => {
                    counter.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Err(TrySendError::Full(ele)) => {
                    // if loops == 0 {
                    //     warn!("net input channel block");
                    // } else
                    if loops == 60 {
                        error!("net input channel block and try with 60 times");
                        loops = 0;
                    }
                    loops += 1;

                    tokio::time::delay_for(Duration::from_secs(1)).await;
                    ele
                }
                Err(TrySendError::Disconnected(_)) => panic!("net input channel Disconnected"),
            }
        }
    }

    // maybe lost data in send/recv buffer
    #[allow(dead_code)]
    pub fn close(self) -> std::io::Result<()> {
        self.stream.shutdown(Shutdown::Both)
    }

    #[allow(dead_code)]
    pub fn close_rough(self) {
        match self.close() {
            Ok(_) => {}
            Err(e) => {
                error!("close client error {}", e);
            }
        }
    }
}

/// Return the `partition_num`
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
