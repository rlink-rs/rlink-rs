use std::borrow::BorrowMut;
use std::net::{Shutdown, SocketAddr};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use futures_util::sink::SinkExt;
use tokio::net::tcp::ReadHalf;
use tokio::net::TcpStream;
use tokio::stream::StreamExt;
use tokio_util::codec::FramedRead;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::codec::{BytesCodec, FramedWrite};

use crate::api::element::{Element, Serde};
use crate::channel::{ElementSender, TrySendError};
use crate::dag::TaskId;
use crate::metrics::{register_counter, Tag};
use crate::net::ResponseCode;

pub(crate) struct Client {
    task_id: TaskId,
    source_task_id: TaskId,
    sender: ElementSender,

    pub(crate) addr: SocketAddr,
    batch_pull_size: u16,
    stream: TcpStream,
}

impl Client {
    pub async fn new(
        task_id: TaskId,
        source_task_id: TaskId,
        sender: ElementSender,
        addr: SocketAddr,
        batch_pull_size: u16,
    ) -> anyhow::Result<Self> {
        match TcpStream::connect(addr).await {
            Ok(stream) => Ok(Client {
                task_id,
                source_task_id,
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
            "Pull remote={}, source={:?}, target={:?}",
            self.addr.to_string(),
            self.source_task_id,
            self.task_id,
        );

        let (r, w) = self.stream.split();
        let mut sink = FramedWrite::new(w, BytesCodec::new());

        let mut codec_framed: FramedRead<ReadHalf, LengthDelimitedCodec> =
            LengthDelimitedCodec::builder()
                .length_field_length(4)
                .new_read(r);

        let tags = vec![
            Tag("job_id".to_string(), self.task_id.job_id.to_string()),
            Tag(
                "partition_num".to_string(),
                self.task_id.task_number.to_string(),
            ),
            Tag(
                "source_job_id".to_string(),
                self.source_task_id.job_id.to_string(),
            ),
        ];
        let counter = Arc::new(AtomicU64::new(0));
        register_counter("NetWorkClient", tags, counter.clone());

        loop {
            let mut buffer = BytesMut::with_capacity(4 + 4 + 4 + 2 + 2);
            buffer.put_u32(12); // 4 + 4 + 2 + 2
            buffer.put_u32(self.source_task_id.job_id);
            buffer.put_u32(self.task_id.job_id);
            buffer.put_u16(self.task_id.task_number);
            buffer.put_u16(self.batch_pull_size);

            sink.send(buffer.to_bytes()).await?;

            loop {
                match codec_framed.next().await {
                    Some(message) => match message {
                        Ok(bytes) => {
                            let (code, element) = frame_parse(bytes);
                            match code {
                                ResponseCode::Ok => {
                                    let element = element.unwrap();
                                    if element.is_watermark() {
                                        debug!(
                                            "net recv Watermark {}",
                                            element.as_watermark().timestamp
                                        );
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
