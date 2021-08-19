use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;

use rlink::core::properties::Properties;

use crate::{OFFSET_BEGIN, OFFSET_END, OFFSET_TYPE};

#[derive(Clone, Debug)]
pub struct PartitionOffset {
    pub(crate) partition: i32,
    pub(crate) offset: i64,
}

impl PartitionOffset {
    pub fn new(partition: i32, offset: i64) -> Self {
        Self { partition, offset }
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }
    pub fn offset(&self) -> i64 {
        self.offset
    }
}

#[derive(Clone, Debug)]
pub enum OffsetRange {
    None,
    Direct {
        begin_offset: HashMap<String, Vec<PartitionOffset>>,
        end_offset: Option<HashMap<String, Vec<PartitionOffset>>>,
    },
    Timestamp {
        begin_timestamp: HashMap<String, u64>,
        end_timestamp: Option<HashMap<String, u64>>,
    },
}

impl Into<Properties> for OffsetRange {
    fn into(self) -> Properties {
        let mut properties = Properties::new();
        match self {
            Self::None => {}
            Self::Direct {
                begin_offset,
                end_offset,
            } => {
                properties.set_str(OFFSET_TYPE, "direct");

                fn add_offset(
                    prefix_key: &str,
                    offset: HashMap<String, Vec<PartitionOffset>>,
                    properties: &mut Properties,
                ) {
                    for (topic, mut po) in offset {
                        po.sort_by_key(|x| x.partition);
                        let offsets: Vec<String> =
                            po.into_iter().map(|x| x.offset.to_string()).collect();
                        let offset_str = offsets.join(",");

                        properties.set_string(format!("{}.{}", prefix_key, topic), offset_str);
                    }
                }

                add_offset(OFFSET_BEGIN, begin_offset, properties.borrow_mut());
                if let Some(end_offset) = end_offset {
                    add_offset(OFFSET_END, end_offset, properties.borrow_mut());
                }
            }
            Self::Timestamp {
                begin_timestamp,
                end_timestamp,
            } => {
                properties.set_str(OFFSET_TYPE, "timestamp");

                fn add_offset(
                    prefix_key: &str,
                    partition_timestamps: HashMap<String, u64>,
                    properties: &mut Properties,
                ) {
                    for (topic, timestamp) in partition_timestamps {
                        properties.set_u64(format!("{}.{}", prefix_key, topic).as_str(), timestamp);
                    }
                }

                add_offset(OFFSET_BEGIN, begin_timestamp, properties.borrow_mut());
                if let Some(end_timestamp) = end_timestamp {
                    add_offset(OFFSET_END, end_timestamp, properties.borrow_mut());
                }
            }
        }

        properties
    }
}

impl TryFrom<Properties> for OffsetRange {
    type Error = anyhow::Error;

    fn try_from(properties: Properties) -> Result<Self, Self::Error> {
        let offset_type = properties.get_string(OFFSET_TYPE).unwrap_or("".to_string());
        let begin = properties.to_sub_properties(OFFSET_BEGIN);
        let end = properties.to_sub_properties(OFFSET_END);

        match offset_type.as_str() {
            "direct" => {
                fn parse(
                    properties: Properties,
                ) -> anyhow::Result<HashMap<String, Vec<PartitionOffset>>> {
                    let mut map = HashMap::new();
                    for (topic, offset_str) in properties.as_map() {
                        let offset_str: Vec<&str> = offset_str.split(",").collect();

                        let mut offsets = Vec::new();
                        for offset_str in offset_str {
                            let offset = i64::from_str(offset_str)?;
                            offsets.push(offset);
                        }

                        let pos: Vec<PartitionOffset> = offsets
                            .into_iter()
                            .enumerate()
                            .map(|(index, offset)| PartitionOffset::new(index as i32, offset))
                            .collect();

                        map.insert(topic.clone(), pos);
                    }
                    Ok(map)
                }

                let begin_offset = parse(begin)?;
                let end_offset = {
                    let end_offset = parse(end)?;
                    if end_offset.is_empty() {
                        None
                    } else {
                        Some(end_offset)
                    }
                };

                Ok(Self::Direct {
                    begin_offset,
                    end_offset,
                })
            }
            "timestamp" => {
                fn parse(properties: Properties) -> anyhow::Result<HashMap<String, u64>> {
                    let mut map = HashMap::new();
                    for (topic, timestamp) in properties.as_map() {
                        let timestamp = u64::from_str(timestamp)?;
                        map.insert(topic.clone(), timestamp);
                    }
                    Ok(map)
                }

                let begin_timestamp = parse(begin)?;
                let end_timestamp = {
                    let end_offset = parse(end)?;
                    if end_offset.is_empty() {
                        None
                    } else {
                        Some(end_offset)
                    }
                };

                Ok(Self::Timestamp {
                    begin_timestamp,
                    end_timestamp,
                })
            }
            "" => Ok(Self::None),
            _ => Err(anyhow!("unknown offset type {}", offset_type)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::convert::TryFrom;

    use rlink::core::properties::Properties;

    use crate::source::offset_range::{OffsetRange, PartitionOffset};

    #[test]
    pub fn properties_convert_test() {
        let mut begin_offset = HashMap::new();
        begin_offset.insert(
            "topic-0".to_string(),
            vec![
                PartitionOffset::new(0, 121),
                PartitionOffset::new(1, 71),
                PartitionOffset::new(2, 78),
            ],
        );
        begin_offset.insert(
            "topic-1".to_string(),
            vec![
                PartitionOffset::new(0, 121),
                PartitionOffset::new(1, 71),
                PartitionOffset::new(2, 78),
            ],
        );

        let mut end_offset = HashMap::new();
        end_offset.insert(
            "topic-0".to_string(),
            vec![
                PartitionOffset::new(0, 137),
                PartitionOffset::new(1, 84),
                PartitionOffset::new(2, 94),
            ],
        );
        end_offset.insert(
            "topic-1".to_string(),
            vec![
                PartitionOffset::new(0, 137),
                PartitionOffset::new(1, 84),
                PartitionOffset::new(2, 94),
            ],
        );

        let offset_range = OffsetRange::Direct {
            begin_offset,
            end_offset: Some(end_offset),
        };

        let properties: Properties = offset_range.into();
        for (k, v) in properties.as_map() {
            println!("{}: {}", k, v);
        }

        let offset_range2 = OffsetRange::try_from(properties).unwrap();

        println!("{:?}", offset_range2)
    }
}
