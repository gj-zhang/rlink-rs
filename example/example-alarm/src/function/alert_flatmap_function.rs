use std::borrow::{Borrow, BorrowMut};
use std::collections::{HashMap, HashSet};
use std::option::Option::Some;
use std::time::Duration;
use dashmap::DashMap;
use rlink::channel::utils::handover::Handover;

use rlink::core::data_types::Schema;
use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{Context, FlatMapFunction, NamedFunction};
use rlink::metrics::Tag;
use rlink::utils;
use rlink::utils::thread::{async_runtime, async_sleep};
use crate::agg::accumulator::Accumulator;

use crate::agg::sum_accumulator::SumAccumulator;
use crate::buffer_gen::{alarm_event, alarm_rule, alarm_rule_event, cleanup_time};
use crate::buffer_gen::alert;

lazy_static! {
    static ref AGG_MAP: DashMap<u32, Vec<f64>> = DashMap::new();
}

#[derive(Function)]
pub struct AlertFlatMapFunction {
    max_window_timestamp: u32,
    handover: Option<Handover>,
}

impl AlertFlatMapFunction {
    pub fn new() -> Self {
        AlertFlatMapFunction {
            max_window_timestamp: u32::MIN,
            handover: None,
        }
    }
}

impl FlatMapFunction for AlertFlatMapFunction {
    fn open(&mut self, context: &Context) -> rlink::core::Result<()> {
        self.handover = Some(Handover::new(self.name(), context.task_id.to_tags(), 10000));


        let task = CleanUpTask::new(self.handover.clone().unwrap());

        utils::thread::spawn("alert-flatmap-clean-state-block", move || {
            async_runtime("alert-flatmap-clean-state").block_on(async {
                task.clean_up().await;
            });
        });
        info!("alert flatmap open success");
        Ok(())
    }

    fn flat_map(&mut self, mut record: Record) -> Box<dyn Iterator<Item=Record>> {
        let rule_event = alarm_rule_event::Entity::parse(record.as_buffer()).unwrap();

        let event_time = rule_event.eventTime;
        let agg_field_names = rule_event.aggregateFieldName;
        let agg_field_names = agg_field_names.split(",");
        let mut agg_field = "";
        for x in agg_field_names {
            if x.eq_ignore_ascii_case("paymentAmount") {
                agg_field = x;
            }
        }

        let mut s = AGG_MAP.entry(event_time).or_insert(Vec::new());
        s.push(rule_event.paymentAmount);

        let function_type = rule_event.aggregatorFunctionType;
        let accumulator = if "sum".eq_ignore_ascii_case(function_type) {
            Some(SumAccumulator::new())
        } else {
            None
        };

        if !rule_event.ruleState.eq_ignore_ascii_case("ACTIVE") {
            return Box::new(vec![].into_iter());
        }

        let window_start_millis = event_time - rule_event.windowMinutes;
        let mut acc = accumulator.unwrap();

        for x in AGG_MAP.iter() {
            let t = x.key();
            if *t > window_start_millis && *t <= event_time {
                let set = x.value();
                for x in set {
                    acc.add(*x);
                }
            }
        }
        let agg_result = acc.get_local_value();

        let alarm = if rule_event.limitOperatorType.eq_ignore_ascii_case("EQUAL") {
            agg_result == rule_event.limit
        } else if rule_event.limitOperatorType.eq_ignore_ascii_case("NOT_EQUAL") {
            agg_result != rule_event.limit
        } else if rule_event.limitOperatorType.eq_ignore_ascii_case("GREATER") {
            agg_result > rule_event.limit
        } else if rule_event.limitOperatorType.eq_ignore_ascii_case("LESS") {
            agg_result < rule_event.limit
        } else if rule_event.limitOperatorType.eq_ignore_ascii_case("LESS_EQUAL") {
            agg_result <= rule_event.limit
        } else if rule_event.limitOperatorType.eq_ignore_ascii_case("GREATER_EQUAL") {
            agg_result >= rule_event.limit
        } else {
            unreachable!("unsupported operator type {}", rule_event.limitOperatorType)
        };

        if alarm {
            let alarm_content = format!("Rule {} | {} : {} -> {}", rule_event.ruleId, rule_event.groupingKeyNames, agg_result, alarm);
            info!("the alarm content: {}", alarm_content);
        }

        let violated_rule = alarm_rule::Entity {
            ruleId: rule_event.ruleId,
            ruleState: rule_event.ruleState,
            groupingKeyNames: rule_event.groupingKeyNames,
            aggregateFieldName: rule_event.aggregateFieldName,
            aggregatorFunctionType: rule_event.aggregatorFunctionType,
            limitOperatorType: rule_event.limitOperatorType,
            limit: rule_event.limit as u32,
            windowMinutes: rule_event.windowMinutes,
        };

        let rule_id = rule_event.ruleId;
        let keys = rule_event.groupingKeyNames;
        let key_splits = keys.split(",");
        let mut key = String::new();
        key.push_str(rule_id.to_string().as_str());
        for x in key_splits {
            key.push_str(x)
        }

        let event = alarm_event::Entity {
            transactionId: rule_event.transactionId,
            eventTime: rule_event.eventTime,
            payeeId: rule_event.payeeId,
            beneficiaryId: rule_event.beneficiaryId,
            paymentAmount: rule_event.paymentAmount,
            paymentType: rule_event.paymentType,
        };
        let rule_json = serde_json::to_string(&violated_rule).unwrap();
        let event_json = serde_json::to_string(&event).unwrap();
        let alert_value = alert::Entity {
            ruleId: rule_event.ruleId,
            violatedRule: rule_json.as_str(),
            key: key.as_str() ,
            triggeringEvent: event_json.as_str(),
            triggeringValue: agg_result,
        };

        let mut record_alert = Record::new();
        alert_value.to_buffer(record_alert.as_buffer()).unwrap();

        // prepare cleanup
        if rule_event.windowMinutes > self.max_window_timestamp {
            self.max_window_timestamp = rule_event.windowMinutes;
        }

        let cleanup_time_threshold = event_time - self.max_window_timestamp;
        let cleanup_entity = cleanup_time::Entity {
            cleanup_time: cleanup_time_threshold,
        };

        let mut cleanup_record = Record::new();
        cleanup_entity.to_buffer(cleanup_record.as_buffer()).unwrap();

        self.handover.as_ref().unwrap().produce(cleanup_record).unwrap();

        Box::new(vec![record_alert].into_iter())
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        info!("close alert flatmap function");
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::Single(Schema::from(&alert::FIELD_METADATA))
    }
}

struct CleanUpTask {
    handover: Handover,
}

impl <'a> CleanUpTask {
    pub fn new(handover: Handover) -> CleanUpTask {
        CleanUpTask {
            handover,
        }
    }

    pub async fn clean_up(self) {
        match self.handover.poll_next() {
            Ok(mut record) => {
                let cleanup = cleanup_time::Entity::parse(record.as_buffer()).unwrap();
                let mut i = 0;
                for x in AGG_MAP.iter() {
                    let k = x.key();
                    if *k < cleanup.cleanup_time {
                        AGG_MAP.remove(k);
                        i += 1;
                    }
                }
                info!("the cleanup handover clean event count: {}", i);
            },
            Err(e) => {
                async_sleep(Duration::from_millis(100)).await;
                warn!("the cleanup handover recv error: {}", e);
            },
        }
    }
}