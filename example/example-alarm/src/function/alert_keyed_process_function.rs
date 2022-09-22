use std::borrow::{Borrow, BorrowMut};
use std::collections::{HashMap, HashSet};
use std::option::Option::Some;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use get_size::GetSize;

use rlink::channel::utils::handover::Handover;
use rlink::core::data_types::Schema;
use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{Context, FlatMapFunction, KeyedProcessFunction, NamedFunction};
use rlink::metrics::Tag;
use rlink::utils;
use rlink::utils::date_time::current_timestamp_millis;
use rlink::utils::thread::{async_runtime, async_sleep};
use rlink_connector_kafka::build_kafka_record;

use crate::agg::accumulator::Accumulator;
use crate::agg::sum_accumulator::SumAccumulator;
use crate::buffer_gen::{alarm_event, alarm_rule, alarm_rule_event, alert, cleanup, dynamic_key};

lazy_static! {
    static ref AGG_MAP: DashMap<String, HashMap<i64, f64>> = DashMap::new();
    static ref MAX_WINDOW_TIMESTAMP: AtomicI32 = AtomicI32::new(i32::MIN);
}

#[derive(Function)]
pub struct AlertKeyedProcessFunction {
    parallelism: u16,
    handover: Option<Handover>,
}

impl AlertKeyedProcessFunction {
    pub fn new(parallelism: u16) -> Self {
        AlertKeyedProcessFunction {
            parallelism,
            handover: None,
        }
    }

    fn process_0(&self, mut key: Record, mut record: Record) -> rlink::core::Result<Option<Record>> {
        let rule_event = alarm_rule_event::Entity::parse(record.as_buffer()).unwrap();
        let rule_key = dynamic_key::Entity::parse(key.as_buffer()).unwrap().dynamic_key.to_string();
        let event_window_millis = rule_event.windowMinutes * 1000;
        let event_time = rule_event.eventTime;
        let event_time_mod = event_time / 1000 * 1000;
        let agg_field_names = rule_event.aggregateFieldName;
        let agg_field_names = agg_field_names.split(",");
        let mut agg_field = "";
        for x in agg_field_names {
            if x.eq_ignore_ascii_case("paymentAmount") {
                agg_field = x;
            }
        }

        {
            let mut s = AGG_MAP.entry(rule_key.clone()).or_insert(HashMap::new());
            s.entry(event_time_mod).or_insert(rule_event.paymentAmount);
        }

        let function_type = rule_event.aggregatorFunctionType;
        let accumulator = if "sum".eq_ignore_ascii_case(function_type) {
            Some(SumAccumulator::new())
        } else {
            None
        };

        if !rule_event.ruleState.eq_ignore_ascii_case("ACTIVE") {
            return Ok(None);
        }

        let window_start_millis = event_time - event_window_millis as i64;
        let mut acc = accumulator.unwrap();

        {
            let entry = AGG_MAP.get(&rule_key).unwrap();
            let hash_map = entry.value();
            for (t, x) in hash_map.iter() {
                if *t > window_start_millis && *t <= event_time_mod {
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
            let alarm_content = format!("Rule {:?} | {} : {} -> {}", rule_event, rule_event.groupingKeyNames, agg_result, alarm);
            if agg_result as i64 % 2 == 0 {
                info!("the alarm content: {}", alarm_content);
            }
        }

        let violated_rule = Rule {
            ruleId: rule_event.ruleId,
            ruleState: rule_event.ruleState.to_string(),
            groupingKeyNames: rule_event.groupingKeyNames.to_string(),
            aggregateFieldName: rule_event.aggregateFieldName.to_string(),
            aggregatorFunctionType: rule_event.aggregatorFunctionType.to_string(),
            limitOperatorType: rule_event.limitOperatorType.to_string(),
            limit: rule_event.limit as u32,
            windowMinutes: rule_event.windowMinutes as i32,
        };
        let event = Event {
            transactionId: rule_event.transactionId,
            eventTime: rule_event.eventTime,
            payeeId: rule_event.payeeId,
            beneficiaryId: rule_event.beneficiaryId,
            paymentAmount: rule_event.paymentAmount,
            paymentType: rule_event.paymentType.to_string(),
        };

        let alert_value = AlertValue {
            ruleId: rule_event.ruleId,
            violatedRule: violated_rule,
            triggeringEvent: event,
            triggeringValue: agg_result,
        };
        let v = serde_json::to_string(&alert_value).unwrap();
        let v = build_kafka_record(current_timestamp_millis() as i64,
                                   "".as_bytes(),
                                   v.as_bytes(),
                                   "alarm_zgj_alerts", 0, 0)
            .unwrap();


        // prepare cleanup
        if event_window_millis > MAX_WINDOW_TIMESTAMP.load(Ordering::Relaxed) {
            MAX_WINDOW_TIMESTAMP.store(event_window_millis, Ordering::Relaxed);
        }

        // let cleanup_time_threshold = event_time - (MAX_WINDOW_TIMESTAMP.load(Ordering::Relaxed)) as i64;
        // let cleanup_entity = cleanup::Entity {
        //     cleanup_time: cleanup_time_threshold,
        // };
        //
        // let mut cleanup_record = Record::new();
        // cleanup_entity.to_buffer(cleanup_record.as_buffer()).unwrap();
        // self.handover.as_ref().unwrap().produce(cleanup_record).unwrap();
        if alarm {
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }
}

impl KeyedProcessFunction for AlertKeyedProcessFunction {
    fn open(&mut self, context: &Context) -> rlink::core::Result<()> {
        let mut task = CleanUpTask::new();

        utils::thread::spawn("alert-flatmap-clean-state-block", move || {
            async_runtime("alert-flatmap-clean-state").block_on(async {
                task.clean_up().await;
            });
        });
        info!("alert flatmap open success");
        Ok(())
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        info!("close alert flatmap function");
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::Single(Schema::from(&alert::FIELD_METADATA))
    }

    fn process(&self, key: Record, record: Record) -> Option<Record> {
        self.process_0(key, record).unwrap()
    }

    fn parallelism(&self) -> u16 {
        self.parallelism
    }
}

struct CleanUpTask {
    inner_remove_count: i32,
    out_remove_count: i32,
}

impl CleanUpTask {
    pub fn new() -> CleanUpTask {
        CleanUpTask {
            inner_remove_count: 0,
            out_remove_count: 0,
        }
    }

    pub async fn clean_up(mut self) {
        loop {
            let mut value_map = 0;
            let mut bytes = 0;
            let mut max_timestamp = i64::MIN;
            let mut min_timestamp = i64::MAX;
            let mut remove_outer_keys = Vec::new();
            let start = current_timestamp_millis();
            {
                for out in AGG_MAP.iter() {
                    let inner = out.value();
                    bytes += inner.get_size();
                    for (k, v) in inner.iter() {
                        value_map += 1;
                        if *k < min_timestamp {
                            min_timestamp = *k;
                        }
                        if *k > max_timestamp {
                            max_timestamp = *k;
                        }
                    }
                }
            }
            let cleanup_time_threshold = max_timestamp - MAX_WINDOW_TIMESTAMP.load(Ordering::Relaxed) as i64;
            {
                // info!("start agg_map iter_mut");
                for mut out in AGG_MAP.iter_mut() {
                    let mut keys: Vec<i64> = Vec::new();
                    {
                        for (k, v) in out.value().iter() {
                            if *k <= cleanup_time_threshold {
                                keys.push(*k);
                            }
                        }
                    }
                    {
                        // info!("inner. before prepare remove inner map");
                        let s = out.value_mut();
                        for x in keys {
                            // info!("inner. inner prepare remove inner map");
                            self.inner_remove_count += 1;
                            s.remove(&x);
                        }
                        if s.len() == 0 {
                            let s = out.key();
                            remove_outer_keys.push(s.clone());
                        }
                        // info!("inner. before prepare remove inner map");
                    }
                }
            }
            for x in remove_outer_keys {
                AGG_MAP.remove(&x);
                self.out_remove_count += 1;
            }
            // info!("end agg_map iter_mut");
            let end = current_timestamp_millis();
            info!("the cleanup handover clean event min_timestamp: {}, max_timestamp: {}, max_window_millis: {},  cleaned inner count: {},cleaned out count: {}, the agg_map len: {}, value_map: {}, interval: {}ms, agg_map size: {}",
                        min_timestamp, max_timestamp,MAX_WINDOW_TIMESTAMP.load(Ordering::Relaxed), self.inner_remove_count,self.out_remove_count, AGG_MAP.len(), value_map, end - start, bytes);

            async_sleep(Duration::from_millis(3000)).await;
        }
    }

    // pub async fn clean_up(mut self) {
    //     loop {
    //         match self.handover.try_poll_next() {
    //             Ok(mut record) => {
    //                 let cleanup = cleanup::Entity::parse(record.as_buffer()).unwrap();
    //                 let mut value_map = 0;
    //                 let mut value_map_vec = 0;
    //                 {
    //                     // info!("start agg_map iter_mut");
    //                     for mut out in AGG_MAP.iter_mut() {
    //                         let mut keys: Vec<i64> = Vec::new();
    //
    //                         {
    //                             // info!("inner. before out.value().iter(), push key");
    //                             for (k, v) in out.value().iter() {
    //                                 value_map += 1;
    //                                 value_map_vec += v.len();
    //                                 if *k < self.min_timestamp {
    //                                     self.min_timestamp = *k;
    //                                 }
    //                                 // info!("event time : {}, cleanup_time: {}", *k, cleanup.cleanup_time);
    //                                 if *k <= cleanup.cleanup_time {
    //                                     keys.push(*k);
    //                                 }
    //                             }
    //                             // info!("inner.after out.value().iter(), push key");
    //                         }
    //                         {
    //                             // info!("inner. before prepare remove inner map");
    //                             let s = out.value_mut();
    //                             for x in keys {
    //                                 // info!("inner. inner prepare remove inner map");
    //                                 self.i += 1;
    //                                 s.remove(&x);
    //                             }
    //                             // info!("inner. before prepare remove inner map");
    //                         }
    //                     }
    //                     // info!("end agg_map iter_mut");
    //                 }
    //                 if self.i != 0 && self.i % 10000 == 0 {
    //                     info!("the cleanup handover clean event min_timestamp: {}, clean_timestamp: {},  cleaned count: {}, the aggmap size: {}, value_map: {}, value_map_vec: {}",
    //                     self.min_timestamp, cleanup.cleanup_time, self.i, AGG_MAP.len(), value_map, value_map_vec);
    //                 }
    //                 // if self.i == 0 {
    //                 //     async_sleep(Duration::from_secs(1)).await;
    //                 // }
    //             }
    //             Err(e) => {
    //                 async_sleep(Duration::from_millis(1000)).await;
    //                 warn!("the cleanup handover recv error: {}", e);
    //             }
    //         }
    //     }
    // }
}

#[derive(Serialize, Deserialize)]
#[allow(non_snake_case)]
struct AlertValue {
    ruleId: u32,
    violatedRule: Rule,
    triggeringEvent: Event,
    triggeringValue: f64,
}

#[derive(Serialize, Deserialize)]
#[allow(non_snake_case)]
struct Event {
    transactionId: i64,
    eventTime: i64,
    payeeId: i64,
    beneficiaryId: i64,
    paymentAmount: f64,
    paymentType: String,
}

#[derive(Serialize, Deserialize)]
#[allow(non_snake_case)]
pub struct Rule {
    ruleId: u32,
    aggregateFieldName: String,
    aggregatorFunctionType: String,
    groupingKeyNames: String,
    limit: u32,
    limitOperatorType: String,
    ruleState: String,
    windowMinutes: i32,
}

#[cfg(test)]
mod test {
    use dashmap::DashMap;

    #[test]
    fn test() {
        let map = DashMap::new();
        let mut i = 0;
        {
            let mut s = map.entry(1000).or_insert(Vec::new());
            s.push(1000);
        }
        {
            let mut s = map.entry(1000).or_insert(Vec::new());
            s.push(1000);
        }
        {
            let mut s = map.entry(2000).or_insert(Vec::new());
            s.push(1000);
        }
        {
            for x in map.iter() {
                let k = x.key();
                let mut v = x.value();
                if *k > 100 {
                    i += *k;
                }
            }
        }

        for x in map.iter() {
            println!("{:?}, {:?}", *x.key(), *x.value())
        }

        println!("map size: {}", map.len());
    }
}