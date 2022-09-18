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
//! Generated file by schema alarm_event, version 1.3.1

use serbuffer::{types, BufferReader, BufferWriter, Buffer, FieldMetadata};

pub mod index {
    pub const transactionId: usize = 0;
    pub const eventTime: usize = 1;
    pub const payeeId: usize = 2;
    pub const beneficiaryId: usize = 3;
    pub const paymentAmount: usize = 4;
    pub const paymentType: usize = 5;
}

pub const FIELD_TYPE: [u8; 6] = [
    // 0: transactionId
    types::I64,
    // 1: eventTime
    types::I64,
    // 2: payeeId
    types::I64,
    // 3: beneficiaryId
    types::I64,
    // 4: paymentAmount
    types::F64,
    // 5: paymentType
    types::STRING,
];

pub const FIELD_NAME: [&'static str; 6] = [
    // 0: transactionId
    "transactionId",
    // 1: eventTime
    "eventTime",
    // 2: payeeId
    "payeeId",
    // 3: beneficiaryId
    "beneficiaryId",
    // 4: paymentAmount
    "paymentAmount",
    // 5: paymentType
    "paymentType",
];

pub const FIELD_METADATA: FieldMetadata<6> = FieldMetadata::new(&FIELD_TYPE, &FIELD_NAME);

pub struct FieldReader<'a> {
    reader: BufferReader<'a, 'static>,
}

impl<'a> FieldReader<'a> {
    pub fn new(b: &'a mut Buffer) -> Self {
        let reader = b.as_reader(&FIELD_TYPE);
        FieldReader { reader }
    }

    pub fn get_transactionId(&mut self) -> Result<i64, std::io::Error> {
        self.reader.get_i64(0)
    }

    pub fn get_eventTime(&mut self) -> Result<i64, std::io::Error> {
        self.reader.get_i64(1)
    }

    pub fn get_payeeId(&mut self) -> Result<i64, std::io::Error> {
        self.reader.get_i64(2)
    }

    pub fn get_beneficiaryId(&mut self) -> Result<i64, std::io::Error> {
        self.reader.get_i64(3)
    }

    pub fn get_paymentAmount(&mut self) -> Result<f64, std::io::Error> {
        self.reader.get_f64(4)
    }

    pub fn get_paymentType(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(5)
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

    pub fn set_transactionId(&mut self, transactionId: i64) -> Result<(), std::io::Error> {
        if self.writer_pos == 0 {
            self.writer_pos += 1;
            self.writer.set_i64(transactionId)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`transactionId` must be set sequentially"))
        }
    }

    pub fn set_eventTime(&mut self, eventTime: i64) -> Result<(), std::io::Error> {
        if self.writer_pos == 1 {
            self.writer_pos += 1;
            self.writer.set_i64(eventTime)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`eventTime` must be set sequentially"))
        }
    }

    pub fn set_payeeId(&mut self, payeeId: i64) -> Result<(), std::io::Error> {
        if self.writer_pos == 2 {
            self.writer_pos += 1;
            self.writer.set_i64(payeeId)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`payeeId` must be set sequentially"))
        }
    }

    pub fn set_beneficiaryId(&mut self, beneficiaryId: i64) -> Result<(), std::io::Error> {
        if self.writer_pos == 3 {
            self.writer_pos += 1;
            self.writer.set_i64(beneficiaryId)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`beneficiaryId` must be set sequentially"))
        }
    }

    pub fn set_paymentAmount(&mut self, paymentAmount: f64) -> Result<(), std::io::Error> {
        if self.writer_pos == 4 {
            self.writer_pos += 1;
            self.writer.set_f64(paymentAmount)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`paymentAmount` must be set sequentially"))
        }
    }

    pub fn set_paymentType(&mut self, paymentType: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 5 {
            self.writer_pos += 1;
            self.writer.set_str(paymentType)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`paymentType` must be set sequentially"))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entity<'a> {
    pub transactionId: i64,
    pub eventTime: i64,
    pub payeeId: i64,
    pub beneficiaryId: i64,
    pub paymentAmount: f64,
    pub paymentType: &'a str,
}

impl<'a> Entity<'a> {
    pub fn to_buffer(&self, b: &mut Buffer) -> Result<(), std::io::Error> {
        let mut writer = b.as_writer(&FIELD_TYPE);
        
        writer.set_i64(self.transactionId)?;
        writer.set_i64(self.eventTime)?;
        writer.set_i64(self.payeeId)?;
        writer.set_i64(self.beneficiaryId)?;
        writer.set_f64(self.paymentAmount)?;
        writer.set_str(self.paymentType)?;

        Ok(())
    }
    
    pub fn parse(b: &'a mut Buffer) -> Result<Self, std::io::Error> {
        let reader = b.as_reader(&FIELD_TYPE);

        let entity = Entity {
            transactionId: reader.get_i64(0)?,
            eventTime: reader.get_i64(1)?,
            payeeId: reader.get_i64(2)?,
            beneficiaryId: reader.get_i64(3)?,
            paymentAmount: reader.get_f64(4)?,
            paymentType: reader.get_str(5)?,
        };

        Ok(entity)
    }
}
