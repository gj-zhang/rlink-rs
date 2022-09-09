use rlink::core::data_types::{DataType, Field, Schema};
use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{Context, KeySelectorFunction};

#[derive(Function)]
pub struct DynamicKeySelectorFunction {}

impl DynamicKeySelectorFunction {
    pub fn new() -> Self {
        DynamicKeySelectorFunction {}
    }
}

impl KeySelectorFunction for DynamicKeySelectorFunction {
    fn open(&mut self, context: &Context) -> rlink::core::Result<()> {
        Ok(())
    }

    fn get_key(&self, record: &mut Record) -> Record {
        Record::new()
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        Ok(())
    }

    fn key_schema(&self, input_schema: FnSchema) -> FnSchema {
        FnSchema::Single(Schema::new(vec![Field::new("key", DataType::String)]))
    }
}