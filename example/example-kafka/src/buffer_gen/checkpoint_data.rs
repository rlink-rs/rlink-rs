#![allow(unknown_lints)]
#![allow(clippy::all)]

#![allow(unused_attributes)]
#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unused_imports)]
#![allow(unused_results)]
//! Generated file by schema checkpoint_data, version 1.2.4

use serbuffer::{types, BufferReader, BufferWriter, Buffer};

pub mod index {
    pub const ts: usize = 0;
    pub const app: usize = 1;
    pub const count: usize = 2;
}

pub const FIELD_TYPE: [u8; 3] = [
    // 0: ts
    types::U64,
    // 1: app
    types::STRING,
    // 2: count
    types::I64,
];

pub struct FieldReader<'a> {
    reader: BufferReader<'a, 'static>,
}

impl<'a> FieldReader<'a> {
    pub fn new(b: &'a mut Buffer) -> Self {
        let reader = b.as_reader(&FIELD_TYPE);
        FieldReader { reader }
    }

    pub fn get_ts(&mut self) -> Result<u64, std::io::Error> {
        self.reader.get_u64(0)
    }

    pub fn get_app(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(1)
    }

    pub fn get_count(&mut self) -> Result<i64, std::io::Error> {
        self.reader.get_i64(2)
    }
}

pub struct FieldWriter<'a> {
    writer: BufferWriter<'a, 'static>,
    writer_pos: usize,
}

impl<'a> FieldWriter<'a> {
    pub fn new(b: &'a mut Buffer) -> Self {
        let writer = b.as_writer(&FIELD_TYPE);
        FieldWriter {
            writer,
            writer_pos: 0,
        }
    }

    pub fn set_ts(&mut self, ts: u64) -> Result<(), std::io::Error> {
        if self.writer_pos == 0 {
            self.writer_pos += 1;
            self.writer.set_u64(ts)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`ts` must be set sequentially"))
        }
    }

    pub fn set_app(&mut self, app: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 1 {
            self.writer_pos += 1;
            self.writer.set_str(app)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`app` must be set sequentially"))
        }
    }

    pub fn set_count(&mut self, count: i64) -> Result<(), std::io::Error> {
        if self.writer_pos == 2 {
            self.writer_pos += 1;
            self.writer.set_i64(count)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`count` must be set sequentially"))
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entity<'a> {
    pub ts: u64,
    pub app: &'a str,
    pub count: i64,
}

impl<'a> Entity<'a> {
    pub fn to_buffer(&self, b: &mut Buffer) -> Result<(), std::io::Error> {
        let mut writer = b.as_writer(&FIELD_TYPE);
        
        writer.set_u64(self.ts)?;
        writer.set_str(self.app)?;
        writer.set_i64(self.count)?;

        Ok(())
    }
    
    pub fn parse(b: &'a mut Buffer) -> Result<Self, std::io::Error> {
        let reader = b.as_reader(&FIELD_TYPE);

        let entity = Entity {
            ts: reader.get_u64(0)?,
            app: reader.get_str(1)?,
            count: reader.get_i64(2)?,
        };

        Ok(entity)
    }
}
