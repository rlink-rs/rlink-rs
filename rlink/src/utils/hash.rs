use std::io::Cursor;

use murmur3::*;

pub fn hash_code(v: &[u8]) -> std::io::Result<u32> {
    let mut cursor = Cursor::new(v);
    murmur3_32(&mut cursor, 0x19264330)
}
