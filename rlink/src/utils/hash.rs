use murmur3::*;
use std::io::Cursor;

// pub fn hash_code(v: Vec<u8>) -> std::io::Result<u32> {
//     let mut cursor = Cursor::new(v);
//     murmur3_32(&mut cursor, 0x19264330)
// }

pub fn hash_code(v: &[u8]) -> std::io::Result<u32> {
    let mut cursor = Cursor::new(v);
    murmur3_32(&mut cursor, 0x19264330)
}
