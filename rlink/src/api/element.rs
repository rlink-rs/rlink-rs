#![allow(dead_code)]

use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::fmt::Debug;

use bytes::{Buf, BufMut, BytesMut};

use crate::api::runtime::{ChannelKey, CheckpointId};
use crate::api::watermark::MIN_WATERMARK;
use crate::api::window::Window;

lazy_static! {
    static ref EMPTY_VEC: Vec<Window> = Vec::with_capacity(0);
}

pub type Buffer = serbuffer::Buffer;
pub type BufferReader<'a, 'b> = serbuffer::BufferReader<'a, 'b>;
pub type BufferWriter<'a, 'b> = serbuffer::BufferWriter<'a, 'b>;
pub mod types {
    pub use serbuffer::types::*;
}

pub(crate) trait Partition {
    fn partition(&self) -> u16;
    fn set_partition(&mut self, partition: u16);
}

const SER_DE_RECORD: u8 = 1;
const SER_DE_WATERMARK: u8 = 2;
const SER_DE_STREAM_STATUS: u8 = 3;
const SER_DE_BARRIER: u8 = 4;

pub(crate) trait Serde {
    fn capacity(&self) -> usize;
    fn to_bytes(&self) -> BytesMut {
        let mut data = BytesMut::with_capacity(self.capacity());
        self.serialize(data.borrow_mut());
        data
    }
    fn serialize(&self, bytes: &mut BytesMut);
    fn deserialize(bytes: &mut BytesMut) -> Self;
}

#[derive(Clone, Debug, Hash)]
pub struct Record {
    pub partition_num: u16,
    pub(crate) timestamp: u64,

    pub(crate) channel_key: ChannelKey,
    pub(crate) location_windows: Option<Vec<Window>>,
    pub(crate) trigger_window: Option<Window>,

    pub(crate) values: Buffer,
}

impl Ord for Record {
    fn cmp(&self, other: &Self) -> Ordering {
        self.values.as_slice().cmp(other.values.as_slice())
    }
}

impl PartialOrd for Record {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.values.as_slice().partial_cmp(other.values.as_slice())
    }
}

impl Eq for Record {}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        self.values.as_slice().eq(other.values.as_slice())
    }
}

impl Record {
    pub fn new() -> Self {
        Record {
            partition_num: 0,
            timestamp: 0,
            channel_key: ChannelKey::default(),
            location_windows: None,
            trigger_window: None,
            values: Buffer::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Record {
            partition_num: 0,
            timestamp: 0,
            channel_key: ChannelKey::default(),
            location_windows: None,
            trigger_window: None,
            values: Buffer::with_capacity(capacity),
        }
    }

    pub fn arity(&self) -> usize {
        self.values.len()
    }

    pub fn extend(&mut self, record: Record) -> Result<(), std::io::Error> {
        self.values.extend(&record.values)
    }

    pub(crate) fn set_location_windows(&mut self, windows: Vec<Window>) {
        self.location_windows = Some(windows);
    }

    pub(crate) fn location_windows(&self) -> &Vec<Window> {
        self.location_windows.as_ref().unwrap_or(&EMPTY_VEC)
    }

    pub(crate) fn min_location_window(&self) -> Option<&Window> {
        match &self.location_windows {
            Some(windows) => windows.get(0),
            None => None,
        }
    }

    pub(crate) fn max_location_window(&self) -> Option<&Window> {
        match &self.location_windows {
            Some(windows) => {
                if windows.len() > 0 {
                    windows.get(windows.len() - 1)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    pub fn set_window_trigger(&mut self, window: Window) {
        self.trigger_window = Some(window);
    }

    pub fn trigger_window(&self) -> Option<Window> {
        self.trigger_window.clone()
    }

    pub fn as_buffer(&mut self) -> &mut Buffer {
        self.values.borrow_mut()
    }

    pub fn as_reader<'a, 'b>(&'a mut self, data_types: &'b [u8]) -> BufferReader<'a, 'b> {
        self.values.as_reader(data_types)
    }

    pub fn as_writer<'a, 'b>(&'a mut self, data_types: &'b [u8]) -> BufferWriter<'a, 'b> {
        self.values.as_writer(data_types)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }
}

impl Partition for Record {
    fn partition(&self) -> u16 {
        self.partition_num
    }

    fn set_partition(&mut self, partition: u16) {
        self.partition_num = partition;
    }
}

impl Serde for Record {
    fn capacity(&self) -> usize {
        15 + self.values.len()
    }

    fn serialize(&self, bytes: &mut BytesMut) {
        let value_len = self.values.len();

        bytes.put_u8(SER_DE_RECORD);
        bytes.put_u16(self.partition_num);
        bytes.put_u64(self.timestamp);

        bytes.put_u32(value_len as u32);

        let data_slice = self.values.as_slice();
        assert_eq!(data_slice.len(), value_len);

        bytes.put_slice(data_slice);
    }

    fn deserialize(bytes: &mut BytesMut) -> Self {
        let flag = bytes.get_u8();
        assert_eq!(flag, SER_DE_RECORD, "Invalid `Record` flag");

        let partition_num = bytes.get_u16();
        let timestamp = bytes.get_u64();

        let value_len = bytes.get_u32() as usize;
        assert_eq!(bytes.remaining(), value_len);

        let values = bytes.split_to(value_len);

        Record {
            partition_num,
            timestamp,
            channel_key: ChannelKey::default(),
            location_windows: None,
            trigger_window: None,
            values: Buffer::from(values),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct Watermark {
    // for partition routing
    partition_num: u16,

    // for align
    pub(crate) task_number: u16,
    pub(crate) num_tasks: u16,
    pub(crate) status_timestamp: u64,

    // current watermark timestamp
    pub(crate) timestamp: u64,

    pub(crate) channel_key: ChannelKey,

    // watermark timestamp location windows based on assign function
    pub(crate) location_windows: Option<Vec<Window>>,
    pub(crate) downstream: bool,
    pub(crate) drop_windows: Option<Vec<Window>>,
}

impl Watermark {
    pub fn new(
        task_number: u16,
        num_tasks: u16,
        timestamp: u64,
        stream_status: &StreamStatus,
    ) -> Self {
        Watermark {
            partition_num: 0,
            task_number,
            num_tasks,
            status_timestamp: stream_status.timestamp,
            timestamp,
            channel_key: ChannelKey::default(),
            location_windows: None,
            downstream: false,
            drop_windows: None,
        }
    }

    pub(crate) fn set_location_windows(&mut self, windows: Vec<Window>) {
        self.location_windows = Some(windows);
    }

    pub(crate) fn min_location_windows(&self) -> Option<&Window> {
        match &self.location_windows {
            Some(windows) => windows.get(0),
            None => None,
        }
    }

    pub(crate) fn is_min(&self) -> bool {
        self.timestamp == MIN_WATERMARK.timestamp
    }
}

impl Partition for Watermark {
    fn partition(&self) -> u16 {
        self.partition_num
    }

    fn set_partition(&mut self, partition: u16) {
        self.partition_num = partition;
    }
}

impl Serde for Watermark {
    fn capacity(&self) -> usize {
        23
    }

    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_u8(SER_DE_WATERMARK);
        bytes.put_u16(self.partition_num);
        bytes.put_u16(self.task_number);
        bytes.put_u16(self.num_tasks);
        bytes.put_u64(self.status_timestamp);
        bytes.put_u64(self.timestamp);
    }

    fn deserialize(bytes: &mut BytesMut) -> Self {
        let flag = bytes.get_u8();
        assert_eq!(flag, SER_DE_WATERMARK, "Invalid `Watermark` flag");

        let partition_num = bytes.get_u16();
        let task_number = bytes.get_u16();
        let num_tasks = bytes.get_u16();
        let status_timestamp = bytes.get_u64();
        let timestamp = bytes.get_u64();

        Watermark {
            partition_num,
            task_number,
            num_tasks,
            status_timestamp,
            timestamp,
            channel_key: ChannelKey::default(),
            location_windows: None,
            downstream: false,
            drop_windows: None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
pub struct StreamStatus {
    partition_num: u16,
    pub(crate) timestamp: u64,

    pub(crate) channel_key: ChannelKey,

    pub(crate) end: bool,
}

impl StreamStatus {
    pub fn new(timestamp: u64, end: bool) -> Self {
        StreamStatus {
            partition_num: 0,
            timestamp,
            channel_key: ChannelKey::default(),
            end,
        }
    }
}

impl Partition for StreamStatus {
    fn partition(&self) -> u16 {
        self.partition_num
    }

    fn set_partition(&mut self, partition: u16) {
        self.partition_num = partition;
    }
}

impl Serde for StreamStatus {
    fn capacity(&self) -> usize {
        10
    }

    fn serialize(&self, bytes: &mut BytesMut) {
        let end = if self.end { 1 } else { 0 };
        bytes.put_u8(SER_DE_STREAM_STATUS);
        bytes.put_u8(end);
        bytes.put_u64(self.timestamp);
    }

    fn deserialize(bytes: &mut BytesMut) -> Self {
        let flag = bytes.get_u8();
        assert_eq!(flag, SER_DE_STREAM_STATUS, "Invalid `StreamStatus` flag");

        let end = bytes.get_u8();
        let timestamp = bytes.get_u64();

        StreamStatus {
            partition_num: 0,
            timestamp,
            channel_key: ChannelKey::default(),
            end: end == 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Barrier {
    partition_num: u16,
    pub(crate) checkpoint_id: CheckpointId,
}

impl Barrier {
    pub fn new(checkpoint_id: CheckpointId) -> Self {
        Barrier {
            partition_num: 0,
            checkpoint_id,
        }
    }
}

impl Partition for Barrier {
    fn partition(&self) -> u16 {
        self.partition_num
    }

    fn set_partition(&mut self, partition: u16) {
        self.partition_num = partition;
    }
}

impl Serde for Barrier {
    fn capacity(&self) -> usize {
        11
    }

    fn serialize(&self, bytes: &mut BytesMut) {
        bytes.put_u8(SER_DE_BARRIER);
        bytes.put_u16(self.partition_num);
        bytes.put_u64(self.checkpoint_id.0);
    }

    fn deserialize(bytes: &mut BytesMut) -> Self {
        let flag = bytes.get_u8();
        assert_eq!(flag, SER_DE_BARRIER, "Invalid `Barrier` flag");

        let partition_num = bytes.get_u16();
        let checkpoint_id = bytes.get_u64();

        Barrier {
            partition_num,
            checkpoint_id: CheckpointId(checkpoint_id),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Element {
    Record(Record),
    Watermark(Watermark),
    StreamStatus(StreamStatus),
    Barrier(Barrier),
}

impl Element {
    pub fn new(_arity: usize) -> Self {
        Element::Record(Record::new())
    }

    pub(crate) fn new_watermark(
        task_number: u16,
        num_tasks: u16,
        timestamp: u64,
        stream_status: &StreamStatus,
    ) -> Self {
        Element::Watermark(Watermark::new(
            task_number,
            num_tasks,
            timestamp,
            stream_status,
        ))
    }

    pub(crate) fn new_stream_status(timestamp: u64, end: bool) -> Self {
        Element::StreamStatus(StreamStatus::new(timestamp, end))
    }

    pub(crate) fn new_barrier(checkpoint_id: CheckpointId) -> Self {
        Element::Barrier(Barrier::new(checkpoint_id))
    }

    /// Checks whether this element is a record.
    /// return `True`, if this element is a record, false otherwise.
    pub(crate) fn is_record(&self) -> bool {
        match self {
            Element::Record(_) => true,
            _ => false,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn as_record(&self) -> &Record {
        match self {
            Element::Record(record) => record,
            _ => panic!("Element is not Record"),
        }
    }

    pub(crate) fn into_record(self) -> Record {
        match self {
            Element::Record(record) => record,
            _ => panic!("Element is not Record"),
        }
    }

    pub(crate) fn as_record_mut(&mut self) -> &mut Record {
        match self {
            Element::Record(record) => record,
            _ => panic!("Element is not Record"),
        }
    }

    /// Checks whether this element is a watermark.
    /// return `True`, if this element is a watermark, false otherwise.
    pub(crate) fn _is_watermark(&self) -> bool {
        match self {
            Element::Watermark(_) => true,
            _ => false,
        }
    }

    pub(crate) fn as_watermark(&self) -> &Watermark {
        match self {
            Element::Watermark(water_mark) => water_mark,
            _ => panic!("Element is not Watermark"),
        }
    }

    pub(crate) fn _as_watermark_mut(&mut self) -> &mut Watermark {
        match self {
            Element::Watermark(water_mark) => water_mark,
            _ => panic!("Element is not Watermark"),
        }
    }

    /// Checks whether this element is a stream status.
    ///	return `True`, if this element is a stream status, false otherwise.
    pub(crate) fn is_stream_status(&self) -> bool {
        match self {
            Element::StreamStatus(_) => true,
            _ => false,
        }
    }

    pub(crate) fn as_stream_status(&self) -> &StreamStatus {
        match self {
            Element::StreamStatus(stream_status) => stream_status,
            _ => panic!("Element is not StreamStatus"),
        }
    }

    /// Checks whether this element is a Barrier.
    ///	return `True`, if this element is a barrier, false otherwise.
    pub(crate) fn is_barrier(&self) -> bool {
        match self {
            Element::Barrier(_) => true,
            _ => false,
        }
    }

    pub(crate) fn as_barrier(&self) -> &Barrier {
        match self {
            Element::Barrier(barrier) => barrier,
            _ => panic!("Element is not Barrier"),
        }
    }

    pub(crate) fn set_channel_key(&mut self, channel_key: ChannelKey) {
        match self {
            Element::Record(record) => {
                record.channel_key = channel_key;
            }
            Element::Watermark(watermark) => {
                watermark.channel_key = channel_key;
            }
            Element::StreamStatus(stream_status) => {
                stream_status.channel_key = channel_key;
            }
            _ => {}
        }
    }
}

impl Partition for Element {
    fn partition(&self) -> u16 {
        match self {
            Element::Record(record) => record.partition(),
            Element::StreamStatus(stream_status) => stream_status.partition(),
            Element::Watermark(water_mark) => water_mark.partition(),
            Element::Barrier(barrier) => barrier.partition(),
        }
    }

    fn set_partition(&mut self, partition: u16) {
        match self {
            Element::Record(record) => record.set_partition(partition),
            Element::StreamStatus(stream_status) => stream_status.set_partition(partition),
            Element::Watermark(water_mark) => water_mark.set_partition(partition),
            Element::Barrier(barrier) => barrier.set_partition(partition),
        }
    }
}

impl Serde for Element {
    fn capacity(&self) -> usize {
        match self {
            Element::Record(record) => record.capacity(),
            Element::Watermark(watermark) => watermark.capacity(),
            Element::StreamStatus(stream_status) => stream_status.capacity(),
            Element::Barrier(barrier) => barrier.capacity(),
        }
    }

    fn serialize(&self, bytes: &mut BytesMut) {
        match self {
            Element::Record(record) => record.serialize(bytes),
            Element::Watermark(watermark) => watermark.serialize(bytes),
            Element::StreamStatus(stream_status) => stream_status.serialize(bytes),
            Element::Barrier(barrier) => barrier.serialize(bytes),
        }
    }

    fn deserialize(bytes: &mut BytesMut) -> Self {
        let tag = bytes.as_ref()[0];
        match tag {
            SER_DE_RECORD => {
                let record = Record::deserialize(bytes);
                Element::Record(record)
            }
            SER_DE_WATERMARK => {
                let watermark = Watermark::deserialize(bytes);
                Element::Watermark(watermark)
            }
            SER_DE_STREAM_STATUS => {
                let stream_status = StreamStatus::deserialize(bytes);
                Element::StreamStatus(stream_status)
            }
            SER_DE_BARRIER => {
                let barrier = Barrier::deserialize(bytes);
                Element::Barrier(barrier)
            }
            _ => panic!("Unknown tag"),
        }
    }
}

impl From<Record> for Element {
    fn from(record: Record) -> Self {
        Element::Record(record)
    }
}

impl From<Watermark> for Element {
    fn from(watermark: Watermark) -> Self {
        Element::Watermark(watermark)
    }
}

impl From<StreamStatus> for Element {
    fn from(stream_status: StreamStatus) -> Self {
        Element::StreamStatus(stream_status)
    }
}

impl From<Barrier> for Element {
    fn from(barrier: Barrier) -> Self {
        Element::Barrier(barrier)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::BorrowMut;

    use crate::api::element::types;
    use crate::api::element::{Element, Record, Serde, StreamStatus, Watermark};

    #[test]
    pub fn serde_element_record_test() {
        let mut record = Record::new();
        record.partition_num = 2;
        record.timestamp = 3;

        let data_types = vec![types::U32, types::U64, types::I32, types::I64, types::BYTES];
        let mut writer = record.as_writer(&data_types);

        writer.set_u32(10).unwrap();
        writer.set_u64(20).unwrap();
        writer.set_i32(30).unwrap();
        writer.set_i64(40).unwrap();
        writer.set_bytes("abc".as_bytes()).unwrap();

        let record_clone = record.clone();
        let mut reader = record.as_reader(&data_types);

        let element_record = Element::Record(record_clone);
        let mut data = element_record.to_bytes();
        let mut element_record_de = Element::deserialize(data.borrow_mut());

        let mut de_reader = element_record_de.as_record_mut().as_reader(&data_types);
        assert_eq!(reader.get_u32(0).unwrap(), de_reader.get_u32(0).unwrap());
        assert_eq!(reader.get_u64(1).unwrap(), de_reader.get_u64(1).unwrap());
        assert_eq!(reader.get_i32(2).unwrap(), de_reader.get_i32(2).unwrap());
        assert_eq!(reader.get_i64(3).unwrap(), de_reader.get_i64(3).unwrap());
        assert_eq!(
            reader.get_bytes(4).unwrap(),
            de_reader.get_bytes(4).unwrap()
        );
    }

    #[test]
    pub fn serde_element_watermark_test() {
        let status = StreamStatus::new(0, false);
        let mut watermark = Watermark::new(1, 2, 6, &status);
        watermark.partition_num = 2;
        watermark.timestamp = 3;

        let element_watermark = Element::Watermark(watermark.clone());
        let mut data = element_watermark.to_bytes();
        let element_watermark_de = Element::deserialize(data.borrow_mut());

        let de_watermark = element_watermark_de.as_watermark();
        assert_eq!(watermark.timestamp, de_watermark.timestamp);
    }

    #[test]
    pub fn serde_element_stream_status_test() {
        let stream_status = StreamStatus::new(0, true);

        let element_watermark = Element::StreamStatus(stream_status.clone());
        let mut data = element_watermark.to_bytes();
        let element_watermark_de = Element::deserialize(data.borrow_mut());

        let de_watermark = element_watermark_de.as_stream_status();
        assert_eq!(stream_status.end, de_watermark.end);
    }
}
