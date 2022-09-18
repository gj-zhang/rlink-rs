use rdkafka::message::FromBytes;

use rlink::core::checkpoint::CheckpointFunction;
use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{Context, FlatMapFunction, NamedFunction};

use crate::buffer_gen::alarm_event;
use rlink_connector_kafka::buffer_gen::kafka_message;

pub struct EventFlatMapFunction {}

impl EventFlatMapFunction {
    pub fn new() -> Self {
        EventFlatMapFunction {}
    }
}

impl FlatMapFunction for EventFlatMapFunction {
    fn open(&mut self, context: &Context) -> rlink::core::Result<()> {
        Ok(())
    }

    fn flat_map(&mut self, mut record: Record) -> Box<dyn Iterator<Item=Record>> {
        let kafka_message::Entity { payload, .. } = kafka_message::Entity::parse(record.as_buffer()).unwrap();
        let s = str::from_bytes(payload).unwrap();
        let e: Event = serde_json::from_str(s).unwrap();
        let event = alarm_event::Entity {
            transactionId: e.transactionId,
            eventTime: e.eventTime,
            payeeId: e.payeeId,
            beneficiaryId: e.beneficiaryId,
            paymentAmount: e.paymentAmount,
            paymentType: e.paymentType,
        };
        let mut r = Record::new();
        event.to_buffer(r.as_buffer()).unwrap();
        Box::new(vec![r].into_iter())
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        Ok(())
    }

    fn schema(&self, _input_schema: FnSchema) -> FnSchema {
        FnSchema::from(&alarm_event::FIELD_METADATA)
    }
}

impl NamedFunction for EventFlatMapFunction {
    fn name(&self) -> &str {
        "EventFlatMapFunction"
    }
}

impl CheckpointFunction for EventFlatMapFunction {}

#[derive(Serialize, Deserialize)]
#[allow(non_snake_case)]
struct Event<'a> {
    transactionId: i64,
    eventTime: i64,
    payeeId: i64,
    beneficiaryId: i64,
    paymentAmount: f64,
    paymentType: &'a str,
}