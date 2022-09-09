use rlink::core::data_types::{DataType, Field, Schema};
use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{Context, CoProcessFunction};

#[derive(Function)]
pub struct RuleCoProcessFunction {
    schema: Schema,
}

impl RuleCoProcessFunction {
    pub fn new() -> Self {
        RuleCoProcessFunction {
            schema: Schema::empty(),
        }
    }
}

impl CoProcessFunction for RuleCoProcessFunction {
    fn open(&mut self, context: &Context) -> rlink::core::Result<()> {
        Ok(())
    }

    fn process_left(&mut self, record: Record) -> Box<dyn Iterator<Item=Record>> {

        todo!()
    }

    // rule stream data
    fn process_right(&mut self, stream_seq: usize, record: Record) -> Box<dyn Iterator<Item=Record>> {}

    fn close(&mut self) -> rlink::core::Result<()> {
        todo!()
    }

    fn schema(&self, input_schema: FnSchema) -> FnSchema {
        todo!()
    }
}