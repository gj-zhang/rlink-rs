use rlink::core::element::{FnSchema, Record};
use rlink::core::function::{Context, CoProcessFunction};

#[derive(Function)]
pub struct AlertCoProcessFunction {}

impl AlertCoProcessFunction {
    pub fn new() -> Self {
        AlertCoProcessFunction {}
    }
}

impl CoProcessFunction for AlertCoProcessFunction {
    fn open(&mut self, context: &Context) -> rlink::core::Result<()> {
        todo!()
    }

    fn process_left(&mut self, record: Record) -> Box<dyn Iterator<Item=Record>> {
        todo!()
    }

    fn process_right(&mut self, stream_seq: usize, record: Record) -> Box<dyn Iterator<Item=Record>> {
        todo!()
    }

    fn close(&mut self) -> rlink::core::Result<()> {
        todo!()
    }

    fn schema(&self, input_schema: FnSchema) -> FnSchema {
        todo!()
    }
}