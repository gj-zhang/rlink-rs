use std::borrow::BorrowMut;
use std::collections::HashMap;
use serbuffer::types::len;
use rlink::core::data_types::{DataType, Field, Schema};
use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{Context, CoProcessFunction};
use rlink::utils::date_time;
use crate::buffer_gen::alarm_event;
use crate::buffer_gen::alarm_rule;
use crate::buffer_gen::alarm_rule_event;

#[derive(Function)]
pub struct RuleCoProcessFunction {
    rule_map: HashMap<u32, Record>
}

impl RuleCoProcessFunction {
    pub fn new() -> Self {
        RuleCoProcessFunction {
            rule_map: HashMap::new(),
        }
    }
}

impl CoProcessFunction for RuleCoProcessFunction {
    fn open(&mut self, context: &Context) -> rlink::core::Result<()> {
        Ok(())
    }

    fn process_left(&mut self, mut record: Record) -> Box<dyn Iterator<Item=Record>> {
        let event = alarm_event::Entity::parse(record.as_buffer()).unwrap();
        let rule_map = &self.rule_map;
        let mut records = vec![];
        for (rule_id, rec) in rule_map.into_iter() {
            let mut rec_borrow = rec.clone();
            let rule = alarm_rule::Entity::parse(rec_borrow.as_buffer()).unwrap();
            let rule_event = alarm_rule_event::Entity {
                ruleId: *rule_id,
                ruleState: rule.ruleState,
                groupingKeyNames: rule.groupingKeyNames,
                unique: "",
                aggregateFieldName: rule.aggregateFieldName,
                aggregatorFunctionType: rule.aggregatorFunctionType,
                limitOperatorType: rule.limitOperatorType,
                limit: rule.limit as f64,
                windowMinutes: rule.windowMinutes,
                controlType: "",
                transactionId: event.transactionId,
                eventTime: event.eventTime,
                payeeId: event.payeeId,
                beneficiaryId: event.beneficiaryId,
                paymentAmount: event.paymentAmount,
                paymentType: event.paymentType,
                ingestionTimestamp: date_time::current_timestamp_millis()
            };
            let mut record_new = Record::new();
            rule_event.to_buffer(record_new.as_buffer()).unwrap();
            records.push(record_new);
        }
        let iter = records.into_iter();
        Box::new(iter)
    }

    // rule stream data
    fn process_right(&mut self, _stream_seq: usize, mut record: Record) -> Box<dyn Iterator<Item=Record>> {
        info!("data-flow-log rule stream recv");
        let rule = alarm_rule::Entity::parse(record.as_buffer()).unwrap();
        self.rule_map.insert(rule.ruleId, record);
        Box::new(vec![].into_iter())
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        info!("close rule event co_process_function");
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        let schema = Schema::from(&alarm_rule_event::FIELD_METADATA);
        FnSchema::Single(schema)
    }
}