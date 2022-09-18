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
//! Generated file by schema dynamic_key, version 1.3.1

use serbuffer::{types, BufferReader, BufferWriter, Buffer, FieldMetadata};

pub mod index {
    pub const dynamic_key: usize = 0;
}

pub const FIELD_TYPE: [u8; 1] = [
    // 0: dynamic_key
    types::STRING,
];

pub const FIELD_NAME: [&'static str; 1] = [
    // 0: dynamic_key
    "dynamic_key",
];

pub const FIELD_METADATA: FieldMetadata<1> = FieldMetadata::new(&FIELD_TYPE, &FIELD_NAME);

pub struct FieldReader<'a> {
    reader: BufferReader<'a, 'static>,
}

impl<'a> FieldReader<'a> {
    pub fn new(b: &'a mut Buffer) -> Self {
        let reader = b.as_reader(&FIELD_TYPE);
        FieldReader { reader }
    }

    pub fn get_dynamic_key(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(0)
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

    pub fn set_dynamic_key(&mut self, dynamic_key: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 0 {
            self.writer_pos += 1;
            self.writer.set_str(dynamic_key)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`dynamic_key` must be set sequentially"))
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entity<'a> {
    pub dynamic_key: &'a str,
}

impl<'a> Entity<'a> {
    pub fn to_buffer(&self, b: &mut Buffer) -> Result<(), std::io::Error> {
        let mut writer = b.as_writer(&FIELD_TYPE);
        
        writer.set_str(self.dynamic_key)?;

        Ok(())
    }
    
    pub fn parse(b: &'a mut Buffer) -> Result<Self, std::io::Error> {
        let reader = b.as_reader(&FIELD_TYPE);

        let entity = Entity {
            dynamic_key: reader.get_str(0)?,
        };

        Ok(entity)
    }
}
