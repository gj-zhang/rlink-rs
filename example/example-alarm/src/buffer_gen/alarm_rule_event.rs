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
//! Generated file by schema alarm_rule_event, version 1.3.1

use serbuffer::{types, BufferReader, BufferWriter, Buffer, FieldMetadata};

pub mod index {
    pub const ruleId: usize = 0;
    pub const ruleState: usize = 1;
    pub const groupingKeyNames: usize = 2;
    pub const unique: usize = 3;
    pub const aggregateFieldName: usize = 4;
    pub const aggregatorFunctionType: usize = 5;
    pub const limitOperatorType: usize = 6;
    pub const limit: usize = 7;
    pub const windowMinutes: usize = 8;
    pub const controlType: usize = 9;
    pub const transactionId: usize = 10;
    pub const eventTime: usize = 11;
    pub const payeeId: usize = 12;
    pub const beneficiaryId: usize = 13;
    pub const paymentAmount: usize = 14;
    pub const paymentType: usize = 15;
    pub const ingestionTimestamp: usize = 16;
}

pub const FIELD_TYPE: [u8; 17] = [
    // 0: ruleId
    types::U32,
    // 1: ruleState
    types::STRING,
    // 2: groupingKeyNames
    types::STRING,
    // 3: unique
    types::STRING,
    // 4: aggregateFieldName
    types::STRING,
    // 5: aggregatorFunctionType
    types::STRING,
    // 6: limitOperatorType
    types::STRING,
    // 7: limit
    types::F64,
    // 8: windowMinutes
    types::I32,
    // 9: controlType
    types::STRING,
    // 10: transactionId
    types::I64,
    // 11: eventTime
    types::I64,
    // 12: payeeId
    types::I64,
    // 13: beneficiaryId
    types::I64,
    // 14: paymentAmount
    types::F64,
    // 15: paymentType
    types::STRING,
    // 16: ingestionTimestamp
    types::U64,
];

pub const FIELD_NAME: [&'static str; 17] = [
    // 0: ruleId
    "ruleId",
    // 1: ruleState
    "ruleState",
    // 2: groupingKeyNames
    "groupingKeyNames",
    // 3: unique
    "unique",
    // 4: aggregateFieldName
    "aggregateFieldName",
    // 5: aggregatorFunctionType
    "aggregatorFunctionType",
    // 6: limitOperatorType
    "limitOperatorType",
    // 7: limit
    "limit",
    // 8: windowMinutes
    "windowMinutes",
    // 9: controlType
    "controlType",
    // 10: transactionId
    "transactionId",
    // 11: eventTime
    "eventTime",
    // 12: payeeId
    "payeeId",
    // 13: beneficiaryId
    "beneficiaryId",
    // 14: paymentAmount
    "paymentAmount",
    // 15: paymentType
    "paymentType",
    // 16: ingestionTimestamp
    "ingestionTimestamp",
];

pub const FIELD_METADATA: FieldMetadata<17> = FieldMetadata::new(&FIELD_TYPE, &FIELD_NAME);

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

    pub fn get_ruleState(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(1)
    }

    pub fn get_groupingKeyNames(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(2)
    }

    pub fn get_unique(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(3)
    }

    pub fn get_aggregateFieldName(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(4)
    }

    pub fn get_aggregatorFunctionType(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(5)
    }

    pub fn get_limitOperatorType(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(6)
    }

    pub fn get_limit(&mut self) -> Result<f64, std::io::Error> {
        self.reader.get_f64(7)
    }

    pub fn get_windowMinutes(&mut self) -> Result<i32, std::io::Error> {
        self.reader.get_i32(8)
    }

    pub fn get_controlType(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(9)
    }

    pub fn get_transactionId(&mut self) -> Result<i64, std::io::Error> {
        self.reader.get_i64(10)
    }

    pub fn get_eventTime(&mut self) -> Result<i64, std::io::Error> {
        self.reader.get_i64(11)
    }

    pub fn get_payeeId(&mut self) -> Result<i64, std::io::Error> {
        self.reader.get_i64(12)
    }

    pub fn get_beneficiaryId(&mut self) -> Result<i64, std::io::Error> {
        self.reader.get_i64(13)
    }

    pub fn get_paymentAmount(&mut self) -> Result<f64, std::io::Error> {
        self.reader.get_f64(14)
    }

    pub fn get_paymentType(&mut self) -> Result<&str, std::io::Error> {
        self.reader.get_str(15)
    }

    pub fn get_ingestionTimestamp(&mut self) -> Result<u64, std::io::Error> {
        self.reader.get_u64(16)
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

    pub fn set_ruleState(&mut self, ruleState: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 1 {
            self.writer_pos += 1;
            self.writer.set_str(ruleState)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`ruleState` must be set sequentially"))
        }
    }

    pub fn set_groupingKeyNames(&mut self, groupingKeyNames: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 2 {
            self.writer_pos += 1;
            self.writer.set_str(groupingKeyNames)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`groupingKeyNames` must be set sequentially"))
        }
    }

    pub fn set_unique(&mut self, unique: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 3 {
            self.writer_pos += 1;
            self.writer.set_str(unique)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`unique` must be set sequentially"))
        }
    }

    pub fn set_aggregateFieldName(&mut self, aggregateFieldName: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 4 {
            self.writer_pos += 1;
            self.writer.set_str(aggregateFieldName)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`aggregateFieldName` must be set sequentially"))
        }
    }

    pub fn set_aggregatorFunctionType(&mut self, aggregatorFunctionType: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 5 {
            self.writer_pos += 1;
            self.writer.set_str(aggregatorFunctionType)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`aggregatorFunctionType` must be set sequentially"))
        }
    }

    pub fn set_limitOperatorType(&mut self, limitOperatorType: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 6 {
            self.writer_pos += 1;
            self.writer.set_str(limitOperatorType)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`limitOperatorType` must be set sequentially"))
        }
    }

    pub fn set_limit(&mut self, limit: f64) -> Result<(), std::io::Error> {
        if self.writer_pos == 7 {
            self.writer_pos += 1;
            self.writer.set_f64(limit)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`limit` must be set sequentially"))
        }
    }

    pub fn set_windowMinutes(&mut self, windowMinutes: i32) -> Result<(), std::io::Error> {
        if self.writer_pos == 8 {
            self.writer_pos += 1;
            self.writer.set_i32(windowMinutes)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`windowMinutes` must be set sequentially"))
        }
    }

    pub fn set_controlType(&mut self, controlType: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 9 {
            self.writer_pos += 1;
            self.writer.set_str(controlType)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`controlType` must be set sequentially"))
        }
    }

    pub fn set_transactionId(&mut self, transactionId: i64) -> Result<(), std::io::Error> {
        if self.writer_pos == 10 {
            self.writer_pos += 1;
            self.writer.set_i64(transactionId)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`transactionId` must be set sequentially"))
        }
    }

    pub fn set_eventTime(&mut self, eventTime: i64) -> Result<(), std::io::Error> {
        if self.writer_pos == 11 {
            self.writer_pos += 1;
            self.writer.set_i64(eventTime)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`eventTime` must be set sequentially"))
        }
    }

    pub fn set_payeeId(&mut self, payeeId: i64) -> Result<(), std::io::Error> {
        if self.writer_pos == 12 {
            self.writer_pos += 1;
            self.writer.set_i64(payeeId)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`payeeId` must be set sequentially"))
        }
    }

    pub fn set_beneficiaryId(&mut self, beneficiaryId: i64) -> Result<(), std::io::Error> {
        if self.writer_pos == 13 {
            self.writer_pos += 1;
            self.writer.set_i64(beneficiaryId)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`beneficiaryId` must be set sequentially"))
        }
    }

    pub fn set_paymentAmount(&mut self, paymentAmount: f64) -> Result<(), std::io::Error> {
        if self.writer_pos == 14 {
            self.writer_pos += 1;
            self.writer.set_f64(paymentAmount)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`paymentAmount` must be set sequentially"))
        }
    }

    pub fn set_paymentType(&mut self, paymentType: &str) -> Result<(), std::io::Error> {
        if self.writer_pos == 15 {
            self.writer_pos += 1;
            self.writer.set_str(paymentType)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`paymentType` must be set sequentially"))
        }
    }

    pub fn set_ingestionTimestamp(&mut self, ingestionTimestamp: u64) -> Result<(), std::io::Error> {
        if self.writer_pos == 16 {
            self.writer_pos += 1;
            self.writer.set_u64(ingestionTimestamp)
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "`ingestionTimestamp` must be set sequentially"))
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entity<'a> {
    pub ruleId: u32,
    pub ruleState: &'a str,
    pub groupingKeyNames: &'a str,
    pub unique: &'a str,
    pub aggregateFieldName: &'a str,
    pub aggregatorFunctionType: &'a str,
    pub limitOperatorType: &'a str,
    pub limit: f64,
    pub windowMinutes: i32,
    pub controlType: &'a str,
    pub transactionId: i64,
    pub eventTime: i64,
    pub payeeId: i64,
    pub beneficiaryId: i64,
    pub paymentAmount: f64,
    pub paymentType: &'a str,
    pub ingestionTimestamp: u64,
}

impl<'a> Entity<'a> {
    pub fn to_buffer(&self, b: &mut Buffer) -> Result<(), std::io::Error> {
        let mut writer = b.as_writer(&FIELD_TYPE);
        
        writer.set_u32(self.ruleId)?;
        writer.set_str(self.ruleState)?;
        writer.set_str(self.groupingKeyNames)?;
        writer.set_str(self.unique)?;
        writer.set_str(self.aggregateFieldName)?;
        writer.set_str(self.aggregatorFunctionType)?;
        writer.set_str(self.limitOperatorType)?;
        writer.set_f64(self.limit)?;
        writer.set_i32(self.windowMinutes)?;
        writer.set_str(self.controlType)?;
        writer.set_i64(self.transactionId)?;
        writer.set_i64(self.eventTime)?;
        writer.set_i64(self.payeeId)?;
        writer.set_i64(self.beneficiaryId)?;
        writer.set_f64(self.paymentAmount)?;
        writer.set_str(self.paymentType)?;
        writer.set_u64(self.ingestionTimestamp)?;

        Ok(())
    }
    
    pub fn parse(b: &'a mut Buffer) -> Result<Self, std::io::Error> {
        let reader = b.as_reader(&FIELD_TYPE);

        let entity = Entity {
            ruleId: reader.get_u32(0)?,
            ruleState: reader.get_str(1)?,
            groupingKeyNames: reader.get_str(2)?,
            unique: reader.get_str(3)?,
            aggregateFieldName: reader.get_str(4)?,
            aggregatorFunctionType: reader.get_str(5)?,
            limitOperatorType: reader.get_str(6)?,
            limit: reader.get_f64(7)?,
            windowMinutes: reader.get_i32(8)?,
            controlType: reader.get_str(9)?,
            transactionId: reader.get_i64(10)?,
            eventTime: reader.get_i64(11)?,
            payeeId: reader.get_i64(12)?,
            beneficiaryId: reader.get_i64(13)?,
            paymentAmount: reader.get_f64(14)?,
            paymentType: reader.get_str(15)?,
            ingestionTimestamp: reader.get_u64(16)?,
        };

        Ok(entity)
    }
}
