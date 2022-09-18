use rdkafka::message::FromBytes;
use serde_json;

use rlink::core::checkpoint::CheckpointFunction;
use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{Context, FlatMapFunction, NamedFunction};
use rlink_connector_kafka::buffer_gen::kafka_message;

use crate::buffer_gen::alarm_rule;

pub struct BroadcastSerdeFlatMapFunction {
    child_job_parallelism: u16,
}

impl BroadcastSerdeFlatMapFunction {
    pub fn new() -> Self {
        BroadcastSerdeFlatMapFunction {
            child_job_parallelism: 0
        }
    }
}

impl FlatMapFunction for BroadcastSerdeFlatMapFunction {
    fn open(&mut self, context: &Context) -> rlink::core::Result<()> {
        self.child_job_parallelism = context.children_len() as u16;

        Ok(())
    }

    fn flat_map(&mut self, mut record: Record) -> Box<dyn Iterator<Item=Record>> {
        let mut records = Vec::new();
        for partition_num in 0..self.child_job_parallelism {
            let kafka_message::Entity { payload, .. } = kafka_message::Entity::parse(record.as_buffer()).unwrap();
            let s = str::from_bytes(payload).unwrap();
            let v = &s[1..s.len() - 1].replace("\\", "");
            let rule: Rule = serde_json::from_str(v).unwrap();
            let key_names = rule.groupingKeyNames;
            let key_names = key_names.join(",");
            let alarm_rule = alarm_rule::Entity {
                ruleId: rule.ruleId,
                aggregateFieldName: rule.aggregateFieldName,
                aggregatorFunctionType: rule.aggregatorFunctionType,
                groupingKeyNames: key_names.as_str(),
                limit: rule.limit,
                limitOperatorType: rule.limitOperatorType,
                ruleState: rule.ruleState,
                windowMinutes: rule.windowMinutes as i32,
            };
            let mut r1 = Record::new();
            alarm_rule.to_buffer(r1.as_buffer()).unwrap();
            r1.partition_num = partition_num;
            records.push(r1);
        }

        Box::new(records.into_iter())
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::from(&alarm_rule::FIELD_METADATA)
    }
}

impl NamedFunction for BroadcastSerdeFlatMapFunction {
    fn name(&self) -> &str {
        "BroadcastSerdeFlatMapFunction"
    }
}

impl CheckpointFunction for BroadcastSerdeFlatMapFunction {}

#[derive(Serialize, Deserialize, Debug)]
#[allow(non_snake_case)]
struct Rule<'a> {
    ruleId: u32,
    aggregateFieldName: &'a str,
    aggregatorFunctionType: &'a str,
    groupingKeyNames: Vec<&'a str>,
    limit: u32,
    limitOperatorType: &'a str,
    ruleState: &'a str,
    windowMinutes: u32,
}
#[cfg(test)]
mod test {
    use crate::function::broadcast_serde_flatmap_function::Rule;

    #[test]
    pub fn test_deser_json() {
        let s = "{\"ruleId\":1,\"aggregateFieldName\":\"paymentAmount\",\"aggregatorFunctionType\":\"SUM\",\"groupingKeyNames\":[\"payeeId\", \"beneficiaryId\"],\"limit\":20000000,\"limitOperatorType\":\"GREATER\",\"ruleState\":\"ACTIVE\",\"windowMinutes\":20}";
        let rule: Rule = serde_json::from_str(s).unwrap();

        println!("rule: {:?}", rule);
    }

    use serde::Deserialize;

    #[derive(Deserialize, Debug)]
    struct User {
        fingerprint: String,
        location: String,
    }

    #[test]
    fn test1() {
        // The type of `j` is `&str`
        let j = "
        {
            \"fingerprint\": \"0xF9BA143B95FF6D82\",
            \"location\": \"Menlo Park, CA\"
        }";

        let u: User = serde_json::from_str(j).unwrap();
        println!("{:#?}", u);
    }
}
