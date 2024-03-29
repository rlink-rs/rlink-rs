use std::borrow::BorrowMut;
use std::convert::TryFrom;

use bytes::{Buf, BufMut, BytesMut};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::core::element::{Element, Serde};
use crate::core::runtime::ChannelKey;

pub(crate) mod client;
pub(crate) mod server;

pub(crate) use client::subscribe;
pub(crate) use server::publish;
pub(crate) use server::Server;

const HEADER_LEN: usize = 4usize;
const REQUEST_BODY_LEN: usize = 20;

#[derive(Clone, Debug)]
pub struct ElementRequest {
    channel_key: ChannelKey,
    batch_pull_size: u16,
    batch_id: u16,
}

impl Into<BytesMut> for ElementRequest {
    fn into(self) -> BytesMut {
        static PACKAGE_LEN: usize = HEADER_LEN + REQUEST_BODY_LEN;

        let mut buffer = BytesMut::with_capacity(PACKAGE_LEN);
        buffer.put_u32(REQUEST_BODY_LEN as u32);
        self.channel_key.serialize(buffer.borrow_mut());
        buffer.put_u16(self.batch_pull_size);
        buffer.put_u16(self.batch_id);

        assert_eq!(buffer.len(), PACKAGE_LEN);
        buffer
    }
}

impl TryFrom<BytesMut> for ElementRequest {
    type Error = anyhow::Error;

    fn try_from(mut buffer: BytesMut) -> Result<Self, Self::Error> {
        let body_len = buffer.get_u32();
        assert_eq!(buffer.remaining(), body_len as usize);

        if body_len as usize != REQUEST_BODY_LEN {
            return Err(anyhow!(
                "Illegal request body length, expect {}, found {}",
                REQUEST_BODY_LEN,
                body_len
            ));
        }

        let channel_key = ChannelKey::deserialize(buffer.borrow_mut());
        let batch_pull_size = buffer.get_u16();
        let batch_id = buffer.get_u16();

        Ok(ElementRequest {
            channel_key,
            batch_pull_size,
            batch_id,
        })
    }
}

/// Response code
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseCode {
    /// unknown code
    Unknown = 0,
    /// for per user data package
    Ok = 1,
    /// after the special batch, then send a finish batch package with the `BatchFinish` code
    BatchFinish = 2,
    /// there is no data in the channel, then send a package with the `Empty` code
    Empty = 3,
    /// the target channel is closed, then send a package with the `Empty` code
    NoService = 4,
}

impl From<u8> for ResponseCode {
    fn from(v: u8) -> Self {
        match v {
            1 => ResponseCode::Ok,
            2 => ResponseCode::BatchFinish,
            3 => ResponseCode::Empty,
            4 => ResponseCode::NoService,
            _ => ResponseCode::Unknown,
        }
    }
}

impl std::fmt::Display for ResponseCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResponseCode::Ok => write!(f, "OK"),
            ResponseCode::BatchFinish => write!(f, "BatchFinish"),
            ResponseCode::Empty => write!(f, "Empty"),
            ResponseCode::NoService => write!(f, "NoService"),
            ResponseCode::Unknown => write!(f, "Unknown"),
        }
    }
}

#[derive(Debug)]
pub struct ElementResponse {
    code: ResponseCode,
    element: Option<Element>,
}

impl ElementResponse {
    pub fn ok(element: Element) -> Self {
        ElementResponse {
            code: ResponseCode::Ok,
            element: Some(element),
        }
    }

    pub fn end(code: ResponseCode) -> Self {
        ElementResponse {
            code,
            element: None,
        }
    }
}

impl Into<BytesMut> for ElementResponse {
    fn into(self) -> BytesMut {
        let ElementResponse { code, element } = self;

        match code {
            ResponseCode::Ok => {
                let element = element.unwrap();

                let body_len = 1usize + element.capacity();
                let package_len = HEADER_LEN + body_len;

                let mut buffer = bytes::BytesMut::with_capacity(package_len);
                buffer.put_u32(body_len as u32); // (code + body).length
                buffer.put_u8(code as u8);
                element.serialize(buffer.borrow_mut());

                assert_eq!(buffer.len(), package_len);
                buffer
            }
            _ => {
                let body_len = 1usize;
                let package_len = HEADER_LEN + body_len;

                let mut buffer = bytes::BytesMut::with_capacity(package_len);
                buffer.put_u32(body_len as u32); // (code + body).length
                buffer.put_u8(code as u8);

                assert_eq!(buffer.len(), package_len);
                buffer
            }
        }
    }
}

impl TryFrom<BytesMut> for ElementResponse {
    type Error = anyhow::Error;

    fn try_from(mut buffer: BytesMut) -> Result<Self, Self::Error> {
        let body_len = buffer.get_u32();
        assert_eq!(buffer.remaining(), body_len as usize);

        let code_value = buffer.get_u8();
        let code = ResponseCode::from(code_value);
        if let ResponseCode::Unknown = code {
            return Err(anyhow!("found unknown code {}", code_value));
        }

        let element = match code {
            ResponseCode::Ok => Some(Element::deserialize(buffer.borrow_mut())),
            _ => None,
        };

        Ok(ElementResponse { code, element })
    }
}

pub fn new_framed_read(read_half: ReadHalf<'_>) -> FramedRead<ReadHalf<'_>, LengthDelimitedCodec> {
    LengthDelimitedCodec::builder()
        .length_field_offset(0)
        .length_field_length(4)
        .length_adjustment(4) // 数据体截取位置，应和num_skip配置使用，保证frame要全部被读取
        .num_skip(0)
        .max_frame_length(1024 * 1024 * 3)
        .big_endian()
        .new_read(read_half)
}

pub fn new_framed_write(write_half: WriteHalf<'_>) -> FramedWrite<WriteHalf<'_>, BytesCodec> {
    FramedWrite::new(write_half, BytesCodec::new())
}
