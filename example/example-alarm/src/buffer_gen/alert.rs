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
//! Generated file by schema alert, version 1.3.1

use serbuffer::{types, BufferReader, BufferWriter, Buffer, FieldMetadata};

pub mod index {
    pub const ruleId: usize = 0;
    pub const violatedRule: usize = 1;
    pub const key: usize = 2;
    pub const triggeringEvent: usize = 3;
    pub const triggeringValue: usize = 4;
}

pub const FIELD_TYPE: [u8; 5] = [
    // 0: ruleId
    types::U32,
    // 1: violatedRule
    types::STRING,
    // 2: key
    types::STRING,
    // 3: triggeringEvent
    types::STRING,
    // 4: triggeringValue
    types::F64,
];

pub const FIELD_NAME: [&'static str; 5] = [
    // 0: ruleId
    "ruleId",
    // 1: violatedRule
    "violatedRule",
    // 2: key
    "key",
    // 3: triggeringEvent
    "triggeringEvent",
    // 4: triggeringValue
    "triggeringValue",
];

pub const FIELD_METADATA: FieldMetadata<5> = FieldMetadata::new(&FIELD_TYPE, &FIELD_NAME);

pub struct FieldReader<'a> {
    reader: BufferReader<'a, 'static>,
}

impl<'a> FieldReader<'a> {
    pub fn new(b: &'a mut Buffer) -> Self {
        let reader = b.as_reader(&FIELD_TYPE);
        FieldReader { reader }
    }

    pub fn get_ruleId(&mut self) -> Result<u32, std::io::Error> {
        self.reader.get_u32(0)
    }

    pub fn get_violatedRule(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(1)
    }

    pub fn get_key(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(2)
    }

    pub fn get_triggeringEvent(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(3)
    }

    pub fn get_triggeringValue(&mut self) -> Result<f64, std::io::Error> {
        self.reader.get_f64(4)
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

    pub fn set_ruleId(&mut self, ruleId: u32) -> Result<(), std::io::Error> {
        if self.writer_pos == 0 {
            self.writer_pos += 1;
            self.writer.set_u32(ruleId)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`ruleId` must be set sequentially"))
        }
    }

    pub fn set_violatedRule(&mut self, violatedRule: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 1 {
            self.writer_pos += 1;
            self.writer.set_str(violatedRule)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`violatedRule` must be set sequentially"))
        }
    }

    pub fn set_key(&mut self, key: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 2 {
            self.writer_pos += 1;
            self.writer.set_str(key)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`key` must be set sequentially"))
        }
    }

    pub fn set_triggeringEvent(&mut self, triggeringEvent: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 3 {
            self.writer_pos += 1;
            self.writer.set_str(triggeringEvent)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`triggeringEvent` must be set sequentially"))
        }
    }

    pub fn set_triggeringValue(&mut self, triggeringValue: f64) -> Result<(), std::io::Error> {
        if self.writer_pos == 4 {
            self.writer_pos += 1;
            self.writer.set_f64(triggeringValue)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`triggeringValue` must be set sequentially"))
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entity<'a> {
    pub ruleId: u32,
    pub violatedRule: &'a str,
    pub key: &'a str,
    pub triggeringEvent: &'a str,
    pub triggeringValue: f64,
}

impl<'a> Entity<'a> {
    pub fn to_buffer(&self, b: &mut Buffer) -> Result<(), std::io::Error> {
        let mut writer = b.as_writer(&FIELD_TYPE);
        
        writer.set_u32(self.ruleId)?;
        writer.set_str(self.violatedRule)?;
        writer.set_str(self.key)?;
        writer.set_str(self.triggeringEvent)?;
        writer.set_f64(self.triggeringValue)?;

        Ok(())
    }
    
    pub fn parse(b: &'a mut Buffer) -> Result<Self, std::io::Error> {
        let reader = b.as_reader(&FIELD_TYPE);

        let entity = Entity {
            ruleId: reader.get_u32(0)?,
            violatedRule: reader.get_str(1)?,
            key: reader.get_str(2)?,
            triggeringEvent: reader.get_str(3)?,
            triggeringValue: reader.get_f64(4)?,
        };

        Ok(entity)
    }
}
