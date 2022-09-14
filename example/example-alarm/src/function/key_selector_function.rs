use rlink::core::data_types::{DataType, Field, Schema};
use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{Context, KeySelectorFunction};
use crate::buffer_gen::alarm_rule_event;
use crate::buffer_gen::dynamic_key;

#[derive(Function)]
pub struct DynamicKeySelectorFunction {
    schema: Schema
}

impl DynamicKeySelectorFunction {
    pub fn new() -> Self {
        DynamicKeySelectorFunction {
            schema: Schema::empty(),
        }
    }
}

impl KeySelectorFunction for DynamicKeySelectorFunction {
    fn open(&mut self, context: &Context) -> rlink::core::Result<()> {
        self.schema = context.input_schema.clone().into();
        Ok(())
    }

    fn get_key(&self, record: &mut Record) -> Record {
        let rule_event = alarm_rule_event::Entity::parse(record.as_buffer()).unwrap();
        let rule_id = rule_event.ruleId;
        let keys = rule_event.groupingKeyNames;
        let key_splits = keys.split(",");
        let mut key = String::new();
        key.push_str(rule_id.to_string().as_str());
        for x in key_splits {
            key.push_str(x)
        }
        let mut r = Record::new();
        let key = dynamic_key::Entity {
            dynamic_key: key.as_str()
        };
        key.to_buffer(r.as_buffer()).unwrap();
        r
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        Ok(())
    }

    fn key_schema(&self, input_schema: FnSchema) -> FnSchema {
        FnSchema::Single(Schema::new(vec![Field::new("key", DataType::String)]))
    }
}