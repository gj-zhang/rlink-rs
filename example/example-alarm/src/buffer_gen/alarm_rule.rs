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
//! Generated file by schema alarm_rule, version 1.3.1

use serbuffer::{types, BufferReader, BufferWriter, Buffer, FieldMetadata};

pub mod index {
    pub const ruleId: usize = 0;
    pub const aggregateFieldName: usize = 1;
    pub const aggregatorFunctionType: usize = 2;
    pub const groupingKeyNames: usize = 3;
    pub const limit: usize = 4;
    pub const limitOperatorType: usize = 5;
    pub const ruleState: usize = 6;
    pub const windowMinutes: usize = 7;
}

pub const FIELD_TYPE: [u8; 8] = [
    // 0: ruleId
    types::U32,
    // 1: aggregateFieldName
    types::STRING,
    // 2: aggregatorFunctionType
    types::STRING,
    // 3: groupingKeyNames
    types::STRING,
    // 4: limit
    types::U32,
    // 5: limitOperatorType
    types::STRING,
    // 6: ruleState
    types::STRING,
    // 7: windowMinutes
    types::I32,
];

pub const FIELD_NAME: [&'static str; 8] = [
    // 0: ruleId
    "ruleId",
    // 1: aggregateFieldName
    "aggregateFieldName",
    // 2: aggregatorFunctionType
    "aggregatorFunctionType",
    // 3: groupingKeyNames
    "groupingKeyNames",
    // 4: limit
    "limit",
    // 5: limitOperatorType
    "limitOperatorType",
    // 6: ruleState
    "ruleState",
    // 7: windowMinutes
    "windowMinutes",
];

pub const FIELD_METADATA: FieldMetadata<8> = FieldMetadata::new(&FIELD_TYPE, &FIELD_NAME);

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

    pub fn get_aggregateFieldName(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(1)
    }

    pub fn get_aggregatorFunctionType(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(2)
    }

    pub fn get_groupingKeyNames(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(3)
    }

    pub fn get_limit(&mut self) -> Result<u32, std::io::Error> {
        self.reader.get_u32(4)
    }

    pub fn get_limitOperatorType(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(5)
    }

    pub fn get_ruleState(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(6)
    }

    pub fn get_windowMinutes(&mut self) -> Result<i32, std::io::Error> {
        self.reader.get_i32(7)
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

    pub fn set_aggregateFieldName(&mut self, aggregateFieldName: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 1 {
            self.writer_pos += 1;
            self.writer.set_str(aggregateFieldName)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`aggregateFieldName` must be set sequentially"))
        }
    }

    pub fn set_aggregatorFunctionType(&mut self, aggregatorFunctionType: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 2 {
            self.writer_pos += 1;
            self.writer.set_str(aggregatorFunctionType)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`aggregatorFunctionType` must be set sequentially"))
        }
    }

    pub fn set_groupingKeyNames(&mut self, groupingKeyNames: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 3 {
            self.writer_pos += 1;
            self.writer.set_str(groupingKeyNames)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`groupingKeyNames` must be set sequentially"))
        }
    }

    pub fn set_limit(&mut self, limit: u32) -> Result<(), std::io::Error> {
        if self.writer_pos == 4 {
            self.writer_pos += 1;
            self.writer.set_u32(limit)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`limit` must be set sequentially"))
        }
    }

    pub fn set_limitOperatorType(&mut self, limitOperatorType: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 5 {
            self.writer_pos += 1;
            self.writer.set_str(limitOperatorType)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`limitOperatorType` must be set sequentially"))
        }
    }

    pub fn set_ruleState(&mut self, ruleState: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 6 {
            self.writer_pos += 1;
            self.writer.set_str(ruleState)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`ruleState` must be set sequentially"))
        }
    }

    pub fn set_windowMinutes(&mut self, windowMinutes: i32) -> Result<(), std::io::Error> {
        if self.writer_pos == 7 {
            self.writer_pos += 1;
            self.writer.set_i32(windowMinutes)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`windowMinutes` must be set sequentially"))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entity<'a> {
    pub ruleId: u32,
    pub aggregateFieldName: &'a str,
    pub aggregatorFunctionType: &'a str,
    pub groupingKeyNames: &'a str,
    pub limit: u32,
    pub limitOperatorType: &'a str,
    pub ruleState: &'a str,
    pub windowMinutes: i32,
}

impl<'a> Entity<'a> {
    pub fn to_buffer(&self, b: &mut Buffer) -> Result<(), std::io::Error> {
        let mut writer = b.as_writer(&FIELD_TYPE);
        
        writer.set_u32(self.ruleId)?;
        writer.set_str(self.aggregateFieldName)?;
        writer.set_str(self.aggregatorFunctionType)?;
        writer.set_str(self.groupingKeyNames)?;
        writer.set_u32(self.limit)?;
        writer.set_str(self.limitOperatorType)?;
        writer.set_str(self.ruleState)?;
        writer.set_i32(self.windowMinutes)?;

        Ok(())
    }
    
    pub fn parse(b: &'a mut Buffer) -> Result<Self, std::io::Error> {
        let reader = b.as_reader(&FIELD_TYPE);

        let entity = Entity {
            ruleId: reader.get_u32(0)?,
            aggregateFieldName: reader.get_str(1)?,
            aggregatorFunctionType: reader.get_str(2)?,
            groupingKeyNames: reader.get_str(3)?,
            limit: reader.get_u32(4)?,
            limitOperatorType: reader.get_str(5)?,
            ruleState: reader.get_str(6)?,
            windowMinutes: reader.get_i32(7)?,
        };

        Ok(entity)
    }
}
